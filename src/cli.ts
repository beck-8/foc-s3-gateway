#!/usr/bin/env node

/**
 * CLI entry point for FOC S3 Gateway
 */

import { Command } from 'commander'
import { startServer } from './server.js'

const program = new Command()

program.name('foc-s3-gateway').description('S3-compatible gateway for Filecoin Onchain Cloud').version('0.1.0')

program
  .command('serve')
  .description('Start the S3 gateway server')
  .option('-p, --port <port>', 'Port to listen on', '8333')
  .option('-H, --host <host>', 'Host to bind to', '0.0.0.0')
  .option('-k, --private-key <key>', 'Wallet private key (or set PRIVATE_KEY env)')
  .option('-r, --rpc-url <url>', 'RPC URL (or set RPC_URL env)')
  .option('-d, --db-path <path>', 'SQLite database path')
  .option('-n, --network <network>', 'Network: mainnet or calibration', 'calibration')
  .option('-a, --access-key <key>', 'Access key for authentication (or set ACCESS_KEY env)')
  .option('-s, --secret-key <key>', 'Secret key for authentication (or set SECRET_KEY env)')
  .option('-w, --webdav-port <port>', 'WebDAV server port (default: S3 port + 1)')
  .option('-c, --copies <count>', 'Default desired copies for new uploads (or set COPIES env)', '2')
  .option('-e, --encryption', 'Enable client-side encryption (requires --secret-key)')
  .action(async (options) => {
    const privateKey = options.privateKey ?? process.env.PRIVATE_KEY
    if (!privateKey) {
      console.error('Error: --private-key or PRIVATE_KEY env is required')
      process.exit(1)
    }

    const rpcUrl = options.rpcUrl ?? process.env.RPC_URL ?? getRpcUrl(options.network)

    const copiesRaw = options.copies ?? process.env.COPIES ?? '2'
    const copies = Number.parseInt(copiesRaw, 10)
    if (!Number.isInteger(copies) || copies < 1) {
      console.error(`Error: --copies must be an integer >= 1 (received: ${copiesRaw})`)
      process.exit(1)
    }

    await startServer({
      port: Number.parseInt(options.port, 10),
      host: options.host,
      privateKey,
      rpcUrl,
      dbPath: options.dbPath ?? process.env.DB_PATH,
      accessKey: options.accessKey ?? process.env.ACCESS_KEY,
      secretKey: options.secretKey ?? process.env.SECRET_KEY,
      webdavPort: options.webdavPort ? Number.parseInt(options.webdavPort, 10) : undefined,
      copies,
      encryption: options.encryption,
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
