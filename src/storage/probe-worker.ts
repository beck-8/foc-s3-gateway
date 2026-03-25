import type { Logger } from 'pino'
import type { MetadataStore } from './metadata-store.js'
import type { SynapseClient } from './synapse-client.js'

export interface ProbeWorkerOptions {
  metadataStore: MetadataStore
  synapseClient: SynapseClient
  logger: Logger
  /** Interval between probe scans in milliseconds (default: 5000 = 5s) */
  scanIntervalMs?: number
  /** Maximum number of concurrent probe requests (default: 10) */
  concurrency?: number
  /** Minimum interval between probing the same copy (default: 3600000 = 1h) */
  probeIntervalMs?: number
  /** Timeout for one probe request (default: 5000 = 5s) */
  probeTimeoutMs?: number
  /** Consecutive probe failures before marking a copy unhealthy (default: 24) */
  unhealthyFailureThreshold?: number
}

export class ProbeWorker {
  private readonly metadataStore: MetadataStore
  private readonly synapseClient: SynapseClient
  private readonly logger: Logger
  private readonly scanIntervalMs: number
  private readonly concurrency: number
  private readonly probeIntervalMs: number
  private readonly probeTimeoutMs: number
  private readonly unhealthyFailureThreshold: number
  private timer: ReturnType<typeof setTimeout> | undefined
  private running = false
  private activeCount = 0

  constructor(options: ProbeWorkerOptions) {
    this.metadataStore = options.metadataStore
    this.synapseClient = options.synapseClient
    this.logger = options.logger.child({ module: 'probe-worker' })
    this.scanIntervalMs = options.scanIntervalMs ?? 5_000
    this.concurrency = options.concurrency ?? 10
    this.probeIntervalMs = options.probeIntervalMs ?? 3_600_000
    this.probeTimeoutMs = options.probeTimeoutMs ?? 5_000
    this.unhealthyFailureThreshold = options.unhealthyFailureThreshold ?? 24
  }

  start(): void {
    this.logger.info(
      {
        scanIntervalMs: this.scanIntervalMs,
        concurrency: this.concurrency,
        probeIntervalMs: this.probeIntervalMs,
        probeTimeoutMs: this.probeTimeoutMs,
        unhealthyFailureThreshold: this.unhealthyFailureThreshold,
      },
      'probe worker started'
    )
    this.scheduleNext()
  }

  stop(): void {
    if (!this.timer) return
    clearTimeout(this.timer)
    this.timer = undefined
    this.logger.info('probe worker stopped')
  }

  getStatus(): {
    scanIntervalMs: number
    concurrency: number
    probeIntervalMs: number
    probeTimeoutMs: number
    unhealthyFailureThreshold: number
    inProgress: number
  } {
    return {
      scanIntervalMs: this.scanIntervalMs,
      concurrency: this.concurrency,
      probeIntervalMs: this.probeIntervalMs,
      probeTimeoutMs: this.probeTimeoutMs,
      unhealthyFailureThreshold: this.unhealthyFailureThreshold,
      inProgress: this.activeCount,
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
      const candidates = this.metadataStore.getCopyProbeCandidates(this.concurrency, this.probeIntervalMs)
      if (candidates.length > 0) {
        this.logger.debug({ count: candidates.length }, 'starting copy probe batch')
      }

      await Promise.allSettled(
        candidates.map(async (copy) => {
          this.activeCount++
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
            this.activeCount--
          }
        })
      )
    } catch (error) {
      this.logger.error({ error }, 'probe worker cycle error')
    } finally {
      this.running = false
      this.scheduleNext()
    }
  }
}
