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

    // Use the Synapse constructor directly with a viem client
    // Synapse.create() requires chains from @filoz/synapse-core which adds custom props,
    // but the constructor with a viem client works with standard viem chains
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

  async upload(data: Uint8Array): Promise<{ pieceCid: string; size: number }> {
    const synapse = this.getSynapse()

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
    const synapse = this.getSynapse()

    this.logger.info({ pieceCid }, 'downloading from FOC')

    const data = await synapse.storage.download({ pieceCid })

    this.logger.info({ pieceCid, size: data.length }, 'download complete')

    return data
  }

  getAddress(): string {
    const synapse = this.getSynapse()
    return synapse.client.account.address
  }
}
