/**
 * Synapse SDK client wrapper for FOC storage operations.
 *
 * Handles initialization and provides upload/download methods
 * that the S3 routes delegate to.
 */

import { Synapse } from '@filoz/synapse-sdk'
import type { Logger } from 'pino'
import { type Hex, createWalletClient, http } from 'viem'
import { privateKeyToAccount } from 'viem/accounts'
import { filecoin, filecoinCalibration } from 'viem/chains'

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
    const chain = this.network === 'mainnet' ? filecoin : filecoinCalibration
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

  async upload(data: Uint8Array): Promise<UploadResult> {
    const synapse = this.getSynapse()

    this.logger.info({ dataSize: data.length }, 'uploading to FOC')

    const result = await synapse.storage.upload(data)

    const copies: CopyInfo[] = result.copies.map((copy) => ({
      providerId: copy.providerId.toString(),
      dataSetId: copy.dataSetId.toString(),
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
   * Download with fallback strategy:
   *   1. Try each stored retrieval URL directly (primary first, then secondaries)
   *   2. Fall back to Synapse SDK discovery (tries all known providers)
   *
   * Direct URL fetch is faster because it skips provider discovery and chain lookups.
   */
  async download(pieceCid: string, copies?: CopyInfo[]): Promise<Uint8Array> {
    // Sort: primary first, then secondary
    const sorted = copies
      ? [...copies].sort((a, b) => (a.role === 'primary' ? -1 : 1) - (b.role === 'primary' ? -1 : 1))
      : []

    // Try stored retrieval URLs first (fast path — no chain lookups)
    for (const copy of sorted) {
      try {
        this.logger.debug(
          { pieceCid, providerId: copy.providerId, role: copy.role },
          'trying direct download'
        )

        const response = await fetch(copy.retrievalUrl)
        if (response.ok) {
          const data = new Uint8Array(await response.arrayBuffer())
          this.logger.info(
            { pieceCid, providerId: copy.providerId, role: copy.role, size: data.length },
            'direct download succeeded'
          )
          return data
        }

        this.logger.warn(
          { pieceCid, providerId: copy.providerId, status: response.status },
          'direct download failed, trying next'
        )
      } catch (error) {
        this.logger.warn(
          { pieceCid, providerId: copy.providerId, error },
          'direct download error, trying next'
        )
      }
    }

    // Fallback: Synapse SDK discovery (slower but more resilient)
    this.logger.info({ pieceCid }, 'falling back to SDK download')
    const synapse = this.getSynapse()
    const data = await synapse.storage.download({ pieceCid })

    this.logger.info({ pieceCid, size: data.length }, 'SDK download complete')
    return data
  }

  getAddress(): string {
    const synapse = this.getSynapse()
    return synapse.client.account.address
  }
}
