/** Background upload worker — only handles staged object uploads. */

import type { Logger } from 'pino'
import type { EncryptionService } from './encryption-service.js'
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
  /** Optional encryption service — when provided, files are encrypted before upload */
  encryptionService?: EncryptionService | undefined
}

interface PendingItem {
  bucket: string
  key: string
  size: number
  contentType: string
  localPath: string
  desiredCopies: number
}

export class UploadWorker {
  private readonly metadataStore: MetadataStore
  private readonly synapseClient: SynapseClient
  private readonly localStore: LocalStore
  private readonly logger: Logger
  private readonly intervalMs: number
  private readonly concurrency: number
  private readonly encryptionService: EncryptionService | undefined
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
    this.encryptionService = options.encryptionService
  }

  /** Start the periodic upload loop */
  start(): void {
    this.logger.info(
      {
        uploadScanIntervalMs: this.intervalMs,
        uploadConcurrency: this.concurrency,
      },
      'upload worker started'
    )
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

  getStatus(): {
    scanIntervalMs: number
    inProgress: number
    concurrency: number
  } {
    return {
      scanIntervalMs: this.intervalMs,
      inProgress: this.activeCount,
      concurrency: this.concurrency,
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

        const succeeded = results.filter((r) => r.status === 'fulfilled' && r.value === true).length
        const failed = pending.length - succeeded

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
  private async uploadOne(item: PendingItem): Promise<boolean> {
    const { bucket, key, localPath, desiredCopies } = item

    this.activeCount++
    this.metadataStore.markUploading(bucket, key)

    if (!this.localStore.exists(localPath)) {
      this.logger.warn({ bucket, key, localPath }, 'local file missing, marking failed')
      this.metadataStore.markUploadFailed(bucket, key)
      this.activeCount--
      return false
    }

    // Validate file size before upload attempt (1 GiB max for FOC)
    const maxUploadSize = 1_065_353_216
    if (item.size > maxUploadSize) {
      this.logger.error(
        { bucket, key, size: item.size, maxUploadSize },
        'file exceeds maximum upload size, marking failed'
      )
      this.metadataStore.markUploadFailed(bucket, key)
      this.activeCount--
      return false
    }

    try {
      this.logger.info({ bucket, key, size: item.size }, 'uploading to FOC')

      let uploadData: Uint8Array | ReadableStream<Uint8Array>
      let encryptionMeta: string | undefined

      if (this.encryptionService) {
        // Encrypt the file before upload
        const fileBuffer = this.localStore.readFile(localPath)
        const encrypted = await this.encryptionService.encryptBuffer(fileBuffer)
        const meta = await this.encryptionService.getEncryptionMeta(encrypted)
        encryptionMeta = JSON.stringify(meta)
        uploadData = encrypted
      } else {
        // Create a web ReadableStream from the local file
        // controller.enqueue is wrapped in try/catch to prevent crash if the
        // consumer (Synapse SDK) closes the stream early (e.g. on size error)
        const fileStream = this.localStore.createReadStream(localPath)
        uploadData = new ReadableStream<Uint8Array>({
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
      }

      const result = await this.synapseClient.upload(uploadData, { copies: desiredCopies })

      if (result.copies.length < desiredCopies) {
        this.metadataStore.recordPartialUpload(bucket, key, result.pieceCid, result.copies, localPath)
        this.localStore.delete(localPath)
        this.logger.warn(
          { bucket, key, pieceCid: result.pieceCid, desiredCopies, actualCopies: result.copies.length },
          'upload completed with insufficient copies, queued for repair worker'
        )
        return true
      }

      // Success: update metadata and remove local file
      this.metadataStore.completeUpload(bucket, key, result.pieceCid, result.copies, localPath, encryptionMeta)
      this.localStore.delete(localPath)

      this.logger.info(
        { bucket, key, pieceCid: result.pieceCid, copies: result.copies.length },
        'async upload to FOC complete'
      )
      return true
    } catch (error) {
      this.logger.error({ bucket, key, error }, 'async upload failed, will retry')
      this.metadataStore.markUploadFailed(bucket, key)
      return false
    } finally {
      this.activeCount--
    }
  }
}
