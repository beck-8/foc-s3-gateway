/**
 * Synapse SDK client wrapper for FOC storage operations.
 *
 * Handles initialization and provides upload/download methods
 * that the S3 routes delegate to.
 */

import { calibration, mainnet } from '@filoz/synapse-core/chains'
import { Synapse } from '@filoz/synapse-sdk'
import type { Logger } from 'pino'
import { createWalletClient, type Hex, http } from 'viem'
import { privateKeyToAccount } from 'viem/accounts'

export interface SynapseClientOptions {
  privateKey: string
  rpcUrl?: string | undefined
  network?: string | undefined
  logger: Logger
}

/** Copy info from a single provider */
export interface CopyInfo {
  providerId: string
  dataSetId: string
  pieceId: string
  retrievalUrl: string
  role: 'primary' | 'secondary'
}

/** Full upload result with provider details */
export interface UploadResult {
  pieceCid: string
  size: number
  copies: CopyInfo[]
}

export class SynapseClient {
  private synapse: Synapse | undefined
  private readonly privateKey: string
  private readonly rpcUrl: string | undefined
  private readonly network: string
  private readonly logger: Logger

  constructor(options: SynapseClientOptions) {
    this.privateKey = options.privateKey
    this.rpcUrl = options.rpcUrl
    this.network = options.network ?? 'calibration'
    this.logger = options.logger.child({ module: 'synapse-client' })
  }

  private getSynapse(): Synapse {
    if (this.synapse) return this.synapse

    const account = privateKeyToAccount(this.privateKey as Hex)
    const chain = this.network === 'mainnet' ? mainnet : calibration
    const transport = this.rpcUrl ? http(this.rpcUrl) : http()

    const client = createWalletClient({
      account,
      chain,
      transport,
    })

    this.synapse = new Synapse({
      client: client as any,
      source: 'foc-s3-gateway',
    })

    this.logger.info({ address: account.address, network: this.network }, 'synapse SDK initialized')
    return this.synapse
  }

  async upload(data: Uint8Array | ReadableStream<Uint8Array>): Promise<UploadResult> {
    const synapse = this.getSynapse()

    const dataSize = data instanceof Uint8Array ? data.length : undefined
    this.logger.info({ dataSize: dataSize ?? 'streaming' }, 'uploading to FOC')

    const result = await synapse.storage.upload(data)

    const copies: CopyInfo[] = result.copies.map((copy) => ({
      providerId: copy.providerId.toString(),
      dataSetId: copy.dataSetId.toString(),
      pieceId: copy.pieceId.toString(),
      retrievalUrl: copy.retrievalUrl,
      role: copy.role,
    }))

    this.logger.info(
      {
        pieceCid: result.pieceCid.toString(),
        size: result.size,
        copies: copies.length,
        complete: result.complete,
        providers: copies.map((c) => c.providerId),
      },
      'upload complete'
    )

    return {
      pieceCid: result.pieceCid.toString(),
      size: result.size,
      copies,
    }
  }

  /**
   * Download with fallback strategy (streaming):
   *   1. Try each stored retrieval URL directly (primary first, then secondaries)
   *   2. Fall back to Synapse SDK discovery (tries all known providers)
   *
   * Returns a Node.js Readable stream so data is piped directly to the client
   * without buffering entire files in memory.
   */
  async download(
    pieceCid: string,
    copies?: CopyInfo[]
  ): Promise<{ stream: import('node:stream').Readable; contentLength?: number }> {
    const { Readable } = await import('node:stream')

    // Sort: primary first, then secondary
    const sorted = copies
      ? [...copies].sort((a, b) => (a.role === 'primary' ? -1 : 1) - (b.role === 'primary' ? -1 : 1))
      : []

    // Try stored retrieval URLs first (fast path — no chain lookups)
    for (const copy of sorted) {
      try {
        this.logger.debug({ pieceCid, providerId: copy.providerId, role: copy.role }, 'trying direct download')

        const response = await fetch(copy.retrievalUrl)
        if (response.ok && response.body) {
          const clHeader = response.headers.get('content-length')
          const contentLength = clHeader ? Number(clHeader) : undefined
          this.logger.info(
            { pieceCid, providerId: copy.providerId, role: copy.role, contentLength },
            'direct download streaming started'
          )
          const nodeStream = Readable.fromWeb(response.body as import('node:stream/web').ReadableStream)
          const result: { stream: import('node:stream').Readable; contentLength?: number } = { stream: nodeStream }
          if (contentLength !== undefined) result.contentLength = contentLength
          return result
        }

        this.logger.warn(
          { pieceCid, providerId: copy.providerId, status: response.status },
          'direct download failed, trying next'
        )
      } catch (error) {
        this.logger.warn({ pieceCid, providerId: copy.providerId, error }, 'direct download error, trying next')
      }
    }

    // Fallback: Synapse SDK discovery (slower, returns full Uint8Array)
    this.logger.info({ pieceCid }, 'falling back to SDK download')
    const synapse = this.getSynapse()
    const data = await synapse.storage.download({ pieceCid })

    this.logger.info({ pieceCid, size: data.length }, 'SDK download complete, wrapping as stream')
    return { stream: Readable.from(data), contentLength: data.length }
  }

  // ── Delete ────────────────────────────────────────────────────────

  /**
   * Delete a piece from a Service Provider.
   * This sends an EIP-712 signed request to the SP's API.
   */
  async deletePiece(options: { dataSetId: string; pieceId: string; serviceURL: string }): Promise<void> {
    const synapse = this.getSynapse()

    // We need to use the synapse-core low-level schedulePieceDeletion
    // But since it's not exported from the main sdk right now, we can use the inner components
    // Actually, Synapse SDK might expose it or we can import it from synapse-core
    const { schedulePieceDeletion } = await import('@filoz/synapse-core/sp')

    // For clientDataSetId, we need a unique nonce per data set. Usually 1n is fine if we only delete once,
    // but standard practice is Date.now() or similar.
    const clientDataSetId = BigInt(Date.now())

    await schedulePieceDeletion(synapse.client as any, {
      dataSetId: BigInt(options.dataSetId),
      pieceId: BigInt(options.pieceId),
      clientDataSetId,
      serviceURL: options.serviceURL,
    })

    this.logger.debug(
      {
        dataSetId: options.dataSetId,
        pieceId: options.pieceId,
      },
      'piece deletion scheduled on SP'
    )
  }

  getAddress(): string {
    const synapse = this.getSynapse()
    return synapse.client.account.address
  }
}
