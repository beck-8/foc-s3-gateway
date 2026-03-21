/**
 * Synapse SDK client wrapper for FOC storage operations.
 *
 * Handles initialization and provides upload/download methods
 * that the S3 routes delegate to.
 */

import { Synapse } from '@filoz/synapse-sdk'
import type { Logger } from 'pino'

export interface SynapseClientOptions {
  privateKey: string
  rpcUrl?: string
  logger: Logger
}

export class SynapseClient {
  private synapse: Synapse | undefined
  private readonly privateKey: string
  private readonly rpcUrl: string | undefined
  private readonly logger: Logger
  private initPromise: Promise<Synapse> | undefined

  constructor(options: SynapseClientOptions) {
    this.privateKey = options.privateKey
    this.rpcUrl = options.rpcUrl
    this.logger = options.logger.child({ module: 'synapse-client' })
  }

  private async init(): Promise<Synapse> {
    if (this.synapse) return this.synapse

    if (this.initPromise) return this.initPromise

    this.initPromise = Synapse.create({
      privateKey: this.privateKey as `0x${string}`,
      rpcUrl: this.rpcUrl,
    }).then((synapse) => {
      this.synapse = synapse
      this.logger.info('synapse SDK initialized')
      return synapse
    })

    return this.initPromise
  }

  async upload(data: Uint8Array): Promise<{ pieceCid: string; size: number }> {
    const synapse = await this.init()

    this.logger.info({ dataSize: data.length }, 'uploading to FOC')

    const result = await synapse.storage.upload(data)

    this.logger.info(
      {
        pieceCid: result.pieceCid.toString(),
        size: result.size,
        copies: result.copies.length,
        complete: result.complete,
      },
      'upload complete'
    )

    return {
      pieceCid: result.pieceCid.toString(),
      size: result.size,
    }
  }

  async download(pieceCid: string): Promise<Uint8Array> {
    const synapse = await this.init()

    this.logger.info({ pieceCid }, 'downloading from FOC')

    const data = await synapse.storage.download({ pieceCid })

    this.logger.info({ pieceCid, size: data.length }, 'download complete')

    return data
  }

  async getAddress(): Promise<string> {
    const synapse = await this.init()
    return synapse.client.account.address
  }
}
