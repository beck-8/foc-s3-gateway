/**
 * Synapse SDK client wrapper for FOC storage operations.
 *
 * Handles initialization and provides upload/download methods
 * that the S3 routes delegate to.
 */

import { calibration, mainnet } from '@filoz/synapse-core/chains'
import { Synapse } from '@filoz/synapse-sdk'
import { WarmStorageService } from '@filoz/synapse-sdk/warm-storage'
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
  requestedCopies: number
  complete: boolean
}

export class SynapseClient {
  private synapse: Synapse | undefined
  private warmStorageService: WarmStorageService | undefined
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

  private getWarmStorageService(): WarmStorageService {
    if (this.warmStorageService) return this.warmStorageService
    const synapse = this.getSynapse()
    this.warmStorageService = new WarmStorageService({ client: synapse.client as any })
    return this.warmStorageService
  }

  async upload(
    data: Uint8Array | ReadableStream<Uint8Array>,
    options?: { copies?: number | undefined }
  ): Promise<UploadResult> {
    const synapse = this.getSynapse()

    const dataSize = data instanceof Uint8Array ? data.length : undefined
    this.logger.info({ dataSize: dataSize ?? 'streaming', copies: options?.copies ?? 2 }, 'uploading to FOC')

    const result = await synapse.storage.upload(data, options?.copies ? { copies: options.copies } : undefined)

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
        requestedCopies: result.requestedCopies,
        complete: result.complete,
        providers: copies.map((c) => c.providerId),
      },
      'upload complete'
    )

    return {
      pieceCid: result.pieceCid.toString(),
      size: result.size,
      copies,
      requestedCopies: result.requestedCopies,
      complete: result.complete,
    }
  }

  async repairCopies(options: {
    pieceCid: string
    sourceCopies: CopyInfo[]
    excludeProviderIds: string[]
    additionalCopies: number
  }): Promise<CopyInfo[]> {
    const { pieceCid, sourceCopies, excludeProviderIds: excludedIdsRaw, additionalCopies } = options
    if (sourceCopies.length === 0) {
      throw new Error('repairCopies requires at least one source copy')
    }
    if (additionalCopies <= 0) {
      return []
    }

    const synapse = this.getSynapse()
    const excludeProviderIds = excludedIdsRaw
      .map((copy) => {
        try {
          return BigInt(copy)
        } catch {
          return undefined
        }
      })
      .filter((id): id is bigint => id !== undefined)
    const sourceCopy = sourceCopies.find((c) => c.role === 'primary') ?? sourceCopies[0]
    if (!sourceCopy) return []

    const contexts = await synapse.storage.createContexts({
      copies: additionalCopies,
      excludeProviderIds,
    })

    const repaired: CopyInfo[] = []
    for (const context of contexts) {
      try {
        const extraData = await context.presignForCommit([{ pieceCid: pieceCid as any }])
        const pullResult = await context.pull({
          pieces: [pieceCid as any],
          from: sourceCopy.retrievalUrl,
          extraData,
        })
        if (pullResult.status !== 'complete') {
          continue
        }
        const commitResult = await context.commit({
          pieces: [{ pieceCid: pieceCid as any }],
          extraData,
        })
        const pieceId = commitResult.pieceIds[0]
        if (pieceId === undefined) {
          continue
        }

        repaired.push({
          providerId: context.provider.id.toString(),
          dataSetId: commitResult.dataSetId.toString(),
          pieceId: pieceId.toString(),
          retrievalUrl: context.getPieceUrl(pieceCid as any),
          role: 'secondary',
        })
      } catch (error) {
        this.logger.warn({ pieceCid, providerId: context.provider.id.toString(), error }, 'copy repair attempt failed')
      }
    }

    return repaired
  }

  async probeCopy(retrievalUrl: string, timeoutMs = 8_000): Promise<boolean> {
    const controller = new AbortController()
    const timer = setTimeout(() => controller.abort(), timeoutMs)
    try {
      const head = await fetch(retrievalUrl, { method: 'HEAD', signal: controller.signal })
      if (head.ok) {
        return true
      }
      if (head.status === 405 || head.status === 501 || head.status === 403) {
        const get = await fetch(retrievalUrl, {
          method: 'GET',
          headers: { Range: 'bytes=0-0' },
          signal: controller.signal,
        })
        return get.ok || get.status === 206
      }
      return false
    } catch {
      return false
    } finally {
      clearTimeout(timer)
    }
  }

  /**
   * Download with fallback strategy (streaming):
   *   1. Try each stored retrieval URL directly (primary first, then secondaries)
   *   2. Fall back to Synapse SDK discovery (tries all known providers)
   *
   * Returns a Node.js Readable stream so data is piped directly to the client
   * without buffering entire files in memory.
   *
   * @param pieceCid - The PieceCID of the data to download
   * @param copies  - Known retrieval copies (tried first via direct fetch)
   * @param range   - Optional byte range `{ start, end }` (inclusive, 0-indexed)
   */
  async download(
    pieceCid: string,
    copies?: CopyInfo[],
    range?: { start: number; end: number }
  ): Promise<{ stream: import('node:stream').Readable; contentLength?: number }> {
    const { Readable } = await import('node:stream')

    // Build Range header string once
    const rangeHeader = range ? `bytes=${range.start}-${range.end}` : undefined

    // Sort: primary first, then secondary
    const sorted = copies
      ? [...copies].sort((a, b) => (a.role === 'primary' ? -1 : 1) - (b.role === 'primary' ? -1 : 1))
      : []

    // Try stored retrieval URLs first (fast path — no chain lookups)
    for (const copy of sorted) {
      try {
        this.logger.debug({ pieceCid, providerId: copy.providerId, role: copy.role, range }, 'trying direct download')

        const fetchOptions: RequestInit = rangeHeader ? { headers: { Range: rangeHeader } } : {}
        const response = await fetch(copy.retrievalUrl, fetchOptions)
        if ((response.ok || response.status === 206) && response.body) {
          const clHeader = response.headers.get('content-length')
          const contentLength = clHeader ? Number(clHeader) : undefined
          this.logger.info(
            { pieceCid, providerId: copy.providerId, role: copy.role, contentLength, status: response.status },
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

    // If range requested, slice the full buffer
    if (range) {
      const sliced = data.slice(range.start, range.end + 1)
      this.logger.info(
        { pieceCid, size: data.length, rangeSize: sliced.length },
        'SDK download complete (range sliced)'
      )
      return { stream: Readable.from(sliced), contentLength: sliced.length }
    }

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
    const warmStorageService = this.getWarmStorageService()

    // We need to use the synapse-core low-level schedulePieceDeletion
    // But since it's not exported from the main sdk right now, we can use the inner components
    // Actually, Synapse SDK might expose it or we can import it from synapse-core
    const { schedulePieceDeletion } = await import('@filoz/synapse-core/sp')

    const dataSetId = BigInt(options.dataSetId)
    const pieceId = BigInt(options.pieceId)
    const dataSetInfo = await warmStorageService.getDataSet({ dataSetId })
    if (!dataSetInfo) {
      throw new Error(`Data set not found for deletion: ${options.dataSetId}`)
    }
    const clientDataSetId = dataSetInfo.clientDataSetId

    // Extract SP root URL from retrieval URL
    // retrieval_url is like "https://host.com/piece/bafk..." but schedulePieceDeletion needs "https://host.com"
    const spOrigin = new URL(options.serviceURL).origin

    await schedulePieceDeletion(synapse.client as any, {
      dataSetId,
      pieceId,
      clientDataSetId,
      serviceURL: spOrigin,
    })

    this.logger.debug(
      {
        dataSetId: options.dataSetId,
        pieceId: options.pieceId,
        clientDataSetId: clientDataSetId.toString(),
        signer: synapse.client.account.address,
        payer: dataSetInfo.payer,
      },
      'piece deletion scheduled on SP'
    )
  }

  getAddress(): string {
    const synapse = this.getSynapse()
    return synapse.client.account.address
  }
}
