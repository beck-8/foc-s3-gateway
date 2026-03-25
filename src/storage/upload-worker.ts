/**
 * Background upload worker — picks up staged objects and uploads to FOC.
 *
 * Concurrency: runs up to `concurrency` uploads in parallel (default: 10).
 *
 * Failure handling:
 *   - Each failed upload increments `upload_attempts` counter
 *   - Objects with < 10 attempts are retried on next cycle
 *   - After 10 failures the object stays as `failed` and is no longer picked up
 *   - Local file is preserved for failed uploads (data is not lost)
 *
 * On success: updates metadata with pieceCid/copies + deletes local file.
 */

import type { Logger } from 'pino'
import type { LocalStore } from './local-store.js'
import type { MetadataStore } from './metadata-store.js'
import type { SynapseClient } from './synapse-client.js'

export interface UploadWorkerOptions {
  metadataStore: MetadataStore
  synapseClient: SynapseClient
  localStore: LocalStore
  logger: Logger
  /** Interval between upload runs in milliseconds (default: 5000 = 5s) */
  intervalMs?: number
  /** Maximum number of concurrent uploads (default: 10) */
  concurrency?: number
  /** Cooldown before retrying incomplete copy repair (default: 300000 = 5m) */
  repairCooldownMs?: number
  /** Minimum interval between copy health probes (default: 3600000 = 1h) */
  probeIntervalMs?: number
  /** Timeout for a single copy health probe (default: 8000 = 8s) */
  probeTimeoutMs?: number
  /** Consecutive probe failures required before marking a copy unhealthy (default: 24) */
  unhealthyFailureThreshold?: number
}

interface PendingItem {
  bucket: string
  key: string
  size: number
  contentType: string
  localPath: string
  desiredCopies: number
}

interface RepairItem {
  bucket: string
  key: string
  pieceCid: string
  desiredCopies: number
  copiesCount: number
  healthyCopies: number
}

export class UploadWorker {
  private readonly metadataStore: MetadataStore
  private readonly synapseClient: SynapseClient
  private readonly localStore: LocalStore
  private readonly logger: Logger
  private readonly intervalMs: number
  private readonly concurrency: number
  private timer: ReturnType<typeof setTimeout> | undefined
  private running = false
  private activeCount = 0
  private activeRepairCount = 0
  private activeProbeCount = 0
  private readonly repairCooldownMs: number
  private readonly probeIntervalMs: number
  private readonly probeTimeoutMs: number
  private readonly unhealthyFailureThreshold: number
  private readonly repairRetryAfter = new Map<string, number>()

  constructor(options: UploadWorkerOptions) {
    this.metadataStore = options.metadataStore
    this.synapseClient = options.synapseClient
    this.localStore = options.localStore
    this.logger = options.logger.child({ module: 'upload-worker' })
    this.intervalMs = options.intervalMs ?? 5_000
    this.concurrency = options.concurrency ?? 10
    this.repairCooldownMs = options.repairCooldownMs ?? 300_000
    this.probeIntervalMs = options.probeIntervalMs ?? 3_600_000
    this.probeTimeoutMs = options.probeTimeoutMs ?? 8_000
    this.unhealthyFailureThreshold = options.unhealthyFailureThreshold ?? 24
  }

  /** Start the periodic upload loop */
  start(): void {
    this.logger.info({ intervalMs: this.intervalMs, concurrency: this.concurrency }, 'upload worker started')
    this.scheduleNext()
  }

  /** Stop the upload loop */
  stop(): void {
    if (this.timer) {
      clearTimeout(this.timer)
      this.timer = undefined
      this.logger.info('upload worker stopped')
    }
  }

  /** How many uploads are currently in-flight */
  getActiveCount(): number {
    return this.activeCount
  }

  getRepairStatus(): {
    scanIntervalMs: number
    probeIntervalMs: number
    probeTimeoutMs: number
    unhealthyFailureThreshold: number
    cooldownMs: number
    pending: number
    probing: number
    inProgress: number
    coolingDown: number
  } {
    const now = Date.now()
    this.pruneRepairCooldowns(now)
    return {
      scanIntervalMs: this.intervalMs,
      probeIntervalMs: this.probeIntervalMs,
      probeTimeoutMs: this.probeTimeoutMs,
      unhealthyFailureThreshold: this.unhealthyFailureThreshold,
      cooldownMs: this.repairCooldownMs,
      pending: this.metadataStore.getUnderReplicatedCount(),
      probing: this.activeProbeCount,
      inProgress: this.activeRepairCount,
      coolingDown: this.repairRetryAfter.size,
    }
  }

  private scheduleNext(): void {
    this.timer = setTimeout(() => this.runUploadCycle(), this.intervalMs)
  }

  /** Grab up to `concurrency` pending items and upload them in parallel */
  private async runUploadCycle(): Promise<void> {
    if (this.running) {
      this.scheduleNext()
      return
    }

    this.running = true

    try {
      // Fetch a batch of pending uploads
      const pending = this.metadataStore.getPendingUploads(this.concurrency)

      if (pending.length > 0) {
        this.logger.info({ count: pending.length }, 'starting concurrent upload batch')

        // Upload all items in parallel
        const results = await Promise.allSettled(pending.map((item) => this.uploadOne(item)))

        const succeeded = results.filter((r) => r.status === 'fulfilled').length
        const failed = results.filter((r) => r.status === 'rejected').length

        this.logger.info({ succeeded, failed, total: pending.length }, 'upload batch complete')
      }

      await this.runProbeCycle()
      await this.runRepairCycle()
    } catch (error) {
      this.logger.error({ error }, 'upload worker cycle error')
    } finally {
      this.running = false
      this.scheduleNext()
    }
  }

  private async runRepairCycle(): Promise<void> {
    const candidates = this.metadataStore.getUnderReplicatedObjects(this.concurrency)
    if (candidates.length === 0) return

    const now = Date.now()
    const ready = candidates.filter((item) => {
      const repairKey = `${item.bucket}/${item.key}`
      const retryAt = this.repairRetryAfter.get(repairKey)
      return retryAt === undefined || retryAt <= now
    })
    if (ready.length === 0) return

    this.logger.info({ count: ready.length }, 'starting copy repair batch')
    await Promise.allSettled(ready.map((item) => this.repairOne(item)))
  }

  private async runProbeCycle(): Promise<void> {
    const candidates = this.metadataStore.getCopyProbeCandidates(this.concurrency * 2, this.probeIntervalMs)
    if (candidates.length === 0) return

    await Promise.allSettled(
      candidates.map(async (copy) => {
        this.activeProbeCount++
        try {
          const ok = await this.synapseClient.probeCopy(copy.retrievalUrl, this.probeTimeoutMs)
          if (ok) {
            this.metadataStore.recordCopyProbeSuccess(copy.bucket, copy.key, copy.providerId)
          } else {
            this.metadataStore.recordCopyProbeFailure(
              copy.bucket,
              copy.key,
              copy.providerId,
              this.unhealthyFailureThreshold
            )
          }
        } finally {
          this.activeProbeCount--
        }
      })
    )
  }

  private pruneRepairCooldowns(now: number): void {
    for (const [key, retryAt] of this.repairRetryAfter.entries()) {
      if (retryAt <= now) {
        this.repairRetryAfter.delete(key)
      }
    }
  }

  private async repairOne(item: RepairItem): Promise<void> {
    const { bucket, key, pieceCid, desiredCopies } = item
    const repairKey = `${bucket}/${key}`
    this.activeRepairCount++
    try {
      const allCopies = this.metadataStore.getObjectCopies(bucket, key)
      const healthyCopies = this.metadataStore.getHealthyObjectCopies(bucket, key)
      if (healthyCopies.length >= desiredCopies) {
        this.repairRetryAfter.delete(repairKey)
        return
      }

      const sourceCopies = healthyCopies.length > 0 ? healthyCopies : allCopies
      if (sourceCopies.length === 0) {
        this.repairRetryAfter.set(repairKey, Date.now() + this.repairCooldownMs)
        return
      }

      const repairedCopies = await this.synapseClient.repairCopies({
        pieceCid,
        sourceCopies,
        excludeProviderIds: allCopies.map((copy) => copy.providerId),
        additionalCopies: desiredCopies - healthyCopies.length,
      })
      this.metadataStore.updateObjectCopies(bucket, key, pieceCid, repairedCopies)

      const refreshedHealthy = this.metadataStore.getHealthyObjectCopies(bucket, key)

      if (refreshedHealthy.length >= desiredCopies) {
        this.repairRetryAfter.delete(repairKey)
        this.logger.info(
          { bucket, key, pieceCid, desiredCopies, healthyCopies: refreshedHealthy.length },
          'copy repair complete'
        )
      } else {
        this.repairRetryAfter.set(repairKey, Date.now() + this.repairCooldownMs)
        this.logger.warn(
          { bucket, key, pieceCid, desiredCopies, healthyCopies: refreshedHealthy.length },
          'copy repair incomplete, will retry'
        )
      }
    } catch (error) {
      this.repairRetryAfter.set(repairKey, Date.now() + this.repairCooldownMs)
      this.logger.warn({ bucket, key, pieceCid, error }, 'copy repair failed, will retry')
    } finally {
      this.activeRepairCount--
    }
  }

  /** Upload a single staged object to FOC */
  private async uploadOne(item: PendingItem): Promise<void> {
    const { bucket, key, localPath, desiredCopies } = item

    if (!this.localStore.exists(localPath)) {
      this.logger.warn({ bucket, key, localPath }, 'local file missing, marking failed')
      this.metadataStore.markUploadFailed(bucket, key)
      return
    }

    // Validate file size before upload attempt (1 GiB max for FOC)
    const maxUploadSize = 1_065_353_216
    if (item.size > maxUploadSize) {
      this.logger.error(
        { bucket, key, size: item.size, maxUploadSize },
        'file exceeds maximum upload size, marking failed'
      )
      this.metadataStore.markUploadFailed(bucket, key)
      return
    }

    this.activeCount++
    try {
      this.metadataStore.markUploading(bucket, key)
      this.logger.info({ bucket, key, size: item.size }, 'uploading to FOC')

      // Create a web ReadableStream from the local file
      // controller.enqueue is wrapped in try/catch to prevent crash if the
      // consumer (Synapse SDK) closes the stream early (e.g. on size error)
      const fileStream = this.localStore.createReadStream(localPath)
      const webStream = new ReadableStream<Uint8Array>({
        start(controller) {
          fileStream.on('data', (chunk) => {
            try {
              controller.enqueue(new Uint8Array(Buffer.from(chunk)))
            } catch {
              // Stream was closed by consumer — stop reading
              fileStream.destroy()
            }
          })
          fileStream.on('end', () => {
            try {
              controller.close()
            } catch {
              // Already closed
            }
          })
          fileStream.on('error', (err) => {
            try {
              controller.error(err)
            } catch {
              // Already closed/errored
            }
          })
        },
        cancel() {
          fileStream.destroy()
        },
      })

      const result = await this.synapseClient.upload(webStream, { copies: desiredCopies })

      if (result.copies.length < desiredCopies) {
        this.metadataStore.recordPartialUpload(bucket, key, result.pieceCid, result.copies)
        this.localStore.delete(localPath)
        this.logger.warn(
          { bucket, key, pieceCid: result.pieceCid, desiredCopies, actualCopies: result.copies.length },
          'upload completed with insufficient copies, will repair in background'
        )
        return
      }

      // Success: update metadata and remove local file
      this.metadataStore.completeUpload(bucket, key, result.pieceCid, result.copies)
      this.localStore.delete(localPath)

      this.logger.info(
        { bucket, key, pieceCid: result.pieceCid, copies: result.copies.length },
        'async upload to FOC complete'
      )
    } catch (error) {
      this.logger.error({ bucket, key, error }, 'async upload failed, will retry')
      this.metadataStore.markUploadFailed(bucket, key)
    } finally {
      this.activeCount--
    }
  }
}
