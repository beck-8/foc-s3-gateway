import type { Logger } from 'pino'
import type { MetadataStore } from './metadata-store.js'
import type { SynapseClient } from './synapse-client.js'

interface RepairItem {
  bucket: string
  key: string
  pieceCid: string
  desiredCopies: number
}

export interface RepairWorkerOptions {
  metadataStore: MetadataStore
  synapseClient: SynapseClient
  logger: Logger
  /** Interval between repair scans in milliseconds (default: 5000 = 5s) */
  scanIntervalMs?: number
  /** Maximum number of parallel object repairs in one cycle (default: 10) */
  concurrency?: number
  /** Cooldown after failed/incomplete repair for one object (default: 300000 = 5m) */
  cooldownMs?: number
}

export class RepairWorker {
  private readonly metadataStore: MetadataStore
  private readonly synapseClient: SynapseClient
  private readonly logger: Logger
  private readonly scanIntervalMs: number
  private readonly concurrency: number
  private readonly cooldownMs: number
  private readonly retryAfter = new Map<string, number>()
  private timer: ReturnType<typeof setTimeout> | undefined
  private running = false
  private activeCount = 0

  constructor(options: RepairWorkerOptions) {
    this.metadataStore = options.metadataStore
    this.synapseClient = options.synapseClient
    this.logger = options.logger.child({ module: 'repair-worker' })
    this.scanIntervalMs = options.scanIntervalMs ?? 5_000
    this.concurrency = options.concurrency ?? 10
    this.cooldownMs = options.cooldownMs ?? 300_000
  }

  start(): void {
    this.logger.info(
      {
        scanIntervalMs: this.scanIntervalMs,
        concurrency: this.concurrency,
        cooldownMs: this.cooldownMs,
      },
      'repair worker started'
    )
    this.scheduleNext()
  }

  stop(): void {
    if (!this.timer) return
    clearTimeout(this.timer)
    this.timer = undefined
    this.logger.info('repair worker stopped')
  }

  getStatus(): {
    scanIntervalMs: number
    cooldownMs: number
    pending: number
    inProgress: number
    coolingDown: number
    concurrency: number
  } {
    this.pruneCooldowns(Date.now())
    return {
      scanIntervalMs: this.scanIntervalMs,
      cooldownMs: this.cooldownMs,
      pending: this.metadataStore.getUnderReplicatedCount(),
      inProgress: this.activeCount,
      coolingDown: this.retryAfter.size,
      concurrency: this.concurrency,
    }
  }

  private scheduleNext(): void {
    this.timer = setTimeout(() => this.runCycle(), this.scanIntervalMs)
  }

  private async runCycle(): Promise<void> {
    if (this.running) {
      this.scheduleNext()
      return
    }

    this.running = true
    try {
      const candidates = this.metadataStore.getUnderReplicatedObjects(this.concurrency)
      if (candidates.length === 0) return

      const now = Date.now()
      this.pruneCooldowns(now)

      const ready = candidates.filter((item) => {
        const retryAt = this.retryAfter.get(this.repairKey(item.bucket, item.key))
        return retryAt === undefined || retryAt <= now
      })
      if (ready.length === 0) return

      this.logger.info({ count: ready.length }, 'starting copy repair batch')
      await Promise.allSettled(
        ready.map((item) =>
          this.repairOne({
            bucket: item.bucket,
            key: item.key,
            pieceCid: item.pieceCid,
            desiredCopies: item.desiredCopies,
          })
        )
      )
    } catch (error) {
      this.logger.error({ error }, 'repair worker cycle error')
    } finally {
      this.running = false
      this.scheduleNext()
    }
  }

  private async repairOne(item: RepairItem): Promise<void> {
    const { bucket, key, pieceCid, desiredCopies } = item
    const repairKey = this.repairKey(bucket, key)

    this.activeCount++
    try {
      const allCopies = this.metadataStore.getObjectCopies(bucket, key)
      const healthyCopies = this.metadataStore.getHealthyObjectCopies(bucket, key)
      if (healthyCopies.length >= desiredCopies) {
        this.retryAfter.delete(repairKey)
        return
      }

      const sourceCopies = healthyCopies.length > 0 ? healthyCopies : allCopies
      if (sourceCopies.length === 0) {
        this.retryAfter.set(repairKey, Date.now() + this.cooldownMs)
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
        this.retryAfter.delete(repairKey)
        this.logger.info(
          { bucket, key, pieceCid, desiredCopies, healthyCopies: refreshedHealthy.length },
          'copy repair complete'
        )
      } else {
        this.retryAfter.set(repairKey, Date.now() + this.cooldownMs)
        this.logger.warn(
          { bucket, key, pieceCid, desiredCopies, healthyCopies: refreshedHealthy.length },
          'copy repair incomplete, will retry'
        )
      }
    } catch (error) {
      this.retryAfter.set(repairKey, Date.now() + this.cooldownMs)
      this.logger.warn({ bucket, key, pieceCid, error }, 'copy repair failed, will retry')
    } finally {
      this.activeCount--
    }
  }

  private pruneCooldowns(now: number): void {
    for (const [key, retryAt] of this.retryAfter.entries()) {
      if (retryAt <= now) {
        this.retryAfter.delete(key)
      }
    }
  }

  private repairKey(bucket: string, key: string): string {
    return `${bucket}/${key}`
  }
}
