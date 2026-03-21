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
}

interface PendingItem {
  bucket: string
  key: string
  size: number
  contentType: string
  localPath: string
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

  constructor(options: UploadWorkerOptions) {
    this.metadataStore = options.metadataStore
    this.synapseClient = options.synapseClient
    this.localStore = options.localStore
    this.logger = options.logger.child({ module: 'upload-worker' })
    this.intervalMs = options.intervalMs ?? 5_000
    this.concurrency = options.concurrency ?? 10
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
    } catch (error) {
      this.logger.error({ error }, 'upload worker cycle error')
    } finally {
      this.running = false
      this.scheduleNext()
    }
  }

  /** Upload a single staged object to FOC */
  private async uploadOne(item: PendingItem): Promise<void> {
    const { bucket, key, localPath } = item

    if (!this.localStore.exists(localPath)) {
      this.logger.warn({ bucket, key, localPath }, 'local file missing, marking failed')
      this.metadataStore.markUploadFailed(bucket, key)
      return
    }

    this.activeCount++
    try {
      this.metadataStore.markUploading(bucket, key)
      this.logger.info({ bucket, key, size: item.size }, 'uploading to FOC')

      // Create a web ReadableStream from the local file
      const fileStream = this.localStore.createReadStream(localPath)
      const webStream = new ReadableStream<Uint8Array>({
        start(controller) {
          fileStream.on('data', (chunk) => {
            controller.enqueue(new Uint8Array(Buffer.from(chunk)))
          })
          fileStream.on('end', () => controller.close())
          fileStream.on('error', (err) => controller.error(err))
        },
      })

      const result = await this.synapseClient.upload(webStream)

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
