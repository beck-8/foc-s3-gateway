/**
 * Background cleanup worker -- periodically processes pending piece deletions.
 *
 * When an object is deleted and no other object references the same PieceCID,
 * the copies are queued in `pending_deletions`. This worker picks them up
 * and calls the SP API to schedule piece removal.
 *
 * Retry logic: each pending deletion gets up to 5 attempts with 60s between runs.
 */

import type { Logger } from 'pino'
import type { MetadataStore } from './metadata-store.js'
import type { SynapseClient } from './synapse-client.js'

export interface CleanupWorkerOptions {
  metadataStore: MetadataStore
  synapseClient: SynapseClient
  logger: Logger
  /** Interval between cleanup runs in milliseconds (default: 600000 = 10 minutes) */
  intervalMs?: number
}

export class CleanupWorker {
  private readonly metadataStore: MetadataStore
  private readonly synapseClient: SynapseClient
  private readonly logger: Logger
  private readonly intervalMs: number
  private timer: ReturnType<typeof setInterval> | undefined

  constructor(options: CleanupWorkerOptions) {
    this.metadataStore = options.metadataStore
    this.synapseClient = options.synapseClient
    this.logger = options.logger.child({ module: 'cleanup-worker' })
    this.intervalMs = options.intervalMs ?? 600_000 // 10 minutes
  }

  /** Start the periodic cleanup loop */
  start(): void {
    this.logger.info({ intervalMs: this.intervalMs }, 'cleanup worker started')
    // Run once immediately, then periodically
    this.runCleanup()
    this.timer = setInterval(() => this.runCleanup(), this.intervalMs)
  }

  /** Stop the cleanup loop */
  stop(): void {
    if (this.timer) {
      clearInterval(this.timer)
      this.timer = undefined
      this.logger.info('cleanup worker stopped')
    }
  }

  /** Process all pending deletions in batches */
  private async runCleanup(): Promise<void> {
    let processedCount = 0

    while (true) {
      const pending = this.metadataStore.getPendingDeletions(10)
      if (pending.length === 0) break

      this.logger.debug({ count: pending.length }, 'processing pending deletions')

      for (const item of pending) {
        try {
          await this.synapseClient.deletePiece({
            pieceId: item.piece_id,
            dataSetId: item.data_set_id,
            serviceURL: item.retrieval_url,
          })

          this.metadataStore.removePendingDeletion(item.id)
          this.logger.info({ pieceCid: item.piece_cid, providerId: item.provider_id }, 'piece deletion completed')
          processedCount++
        } catch (error) {
          this.metadataStore.incrementDeletionAttempt(item.id)
          this.logger.warn(
            { pieceCid: item.piece_cid, providerId: item.provider_id, attempt: item.attempts + 1, error },
            'piece deletion failed, will retry'
          )
        }
      }
    }

    if (processedCount > 0) {
      this.logger.info({ processedCount }, 'finished batch cleanup of deletions')
    }
  }
}
