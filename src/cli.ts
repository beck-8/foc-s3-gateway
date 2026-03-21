#!/usr/bin/env node

/**
 * CLI entry point for FOC S3 Gateway
 */

import { Command } from 'commander'
import { startServer } from './server.js'

const program = new Command()

program
  .name('foc-s3-gateway')
  .description('S3-compatible gateway for Filecoin Onchain Cloud')
  .version('0.1.0')

program
  .command('serve')
  .description('Start the S3 gateway server')
  .option('-p, --port <port>', 'Port to listen on', '8333')
  .option('-H, --host <host>', 'Host to bind to', '0.0.0.0')
  .option('-k, --private-key <key>', 'Wallet private key (or set PRIVATE_KEY env)')
  .option('-r, --rpc-url <url>', 'RPC URL (or set RPC_URL env)')
  .option('-d, --db-path <path>', 'SQLite database path')
  .option('-n, --network <network>', 'Network: mainnet or calibration', 'calibration')
  .action(async (options) => {
    const privateKey = options.privateKey ?? process.env['PRIVATE_KEY']
    if (!privateKey) {
      console.error('Error: --private-key or PRIVATE_KEY env is required')
      process.exit(1)
    }

    const rpcUrl = options.rpcUrl ?? process.env['RPC_URL'] ?? getRpcUrl(options.network)

    await startServer({
      port: Number.parseInt(options.port, 10),
      host: options.host,
      privateKey,
      rpcUrl,
      dbPath: options.dbPath ?? process.env['DB_PATH'],
    })
  })

function getRpcUrl(network: string): string {
  switch (network) {
    case 'mainnet':
      return 'https://api.node.glif.io/rpc/v1'
    case 'calibration':
      return 'https://api.calibration.node.glif.io/rpc/v1'
    default:
      return 'https://api.calibration.node.glif.io/rpc/v1'
  }
}

program.parse()
