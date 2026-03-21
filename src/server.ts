/**
 * Fastify server that serves S3-compatible API over FOC storage.
 */

import { mkdirSync } from 'node:fs'
import { homedir } from 'node:os'
import path from 'node:path'
import Fastify from 'fastify'
import type { Logger } from 'pino'
import { registerRoutes } from './routes/index.js'
import { MetadataStore } from './storage/metadata-store.js'
import { SynapseClient } from './storage/synapse-client.js'

export interface ServerOptions {
  port: number
  host: string
  privateKey: string
  rpcUrl?: string | undefined
  network?: string | undefined
  dbPath?: string | undefined
}

export async function createServer(options: ServerOptions) {
  const app = Fastify({
    logger: {
      level: 'info',
      transport: {
        target: 'pino-pretty',
        options: { colorize: true },
      },
    },
    // Disable body parsing — we handle raw streams for PutObject
    bodyLimit: 1024 * 1024 * 1024, // 1 GiB
  })

  const logger = app.log as Logger

  // Resolve database path — use platform-specific data dir by default
  const dbPath = options.dbPath ?? getDefaultDbPath()
  mkdirSync(path.dirname(dbPath), { recursive: true })
  logger.info({ dbPath }, 'using database')

  // Initialize storage
  const metadataStore = new MetadataStore({ dbPath, logger })
  const synapseClient = new SynapseClient({
    privateKey: options.privateKey,
    rpcUrl: options.rpcUrl,
    network: options.network,
    logger,
  })

  // Disable Fastify's default content type parser for all types
  // so we can handle raw request bodies ourselves
  app.removeAllContentTypeParsers()
  app.addContentTypeParser('*', function (_request, _payload, done) {
    done(null)
  })

  // Register routes
  registerRoutes(app, { metadataStore, synapseClient, logger })

  // Graceful shutdown
  app.addHook('onClose', () => {
    metadataStore.close()
  })

  return { app, metadataStore, synapseClient }
}

export async function startServer(options: ServerOptions): Promise<void> {
  const { app } = await createServer(options)

  try {
    await app.listen({ port: options.port, host: options.host })

    const address = app.server.address()
    const addressStr = typeof address === 'string' ? address : `${address?.address}:${address?.port}`

    app.log.info(`
╔══════════════════════════════════════════════════════════════╗
║  FOC S3 Gateway is running!                                  ║
║                                                              ║
║  Endpoint: http://${addressStr.padEnd(40)}║
║                                                              ║
║  Rclone config:                                              ║
║    [foc]                                                     ║
║    type = s3                                                 ║
║    provider = Other                                          ║
║    endpoint = http://${addressStr.padEnd(40)}║
║    access_key_id = any                                       ║
║    secret_access_key = any                                   ║
║                                                              ║
║  Usage:                                                      ║
║    rclone ls foc:default                                     ║
║    rclone copy ./file.txt foc:default/                       ║
║    rclone mount foc:default /mnt/foc --vfs-cache-mode full   ║
╚══════════════════════════════════════════════════════════════╝
`)
  } catch (error) {
    app.log.error(error)
    process.exit(1)
  }
}

/** Platform-specific default data directory for the database */
function getDefaultDbPath(): string {
  const appName = 'foc-s3-gateway'

  switch (process.platform) {
    case 'darwin':
      return path.join(homedir(), 'Library', 'Application Support', appName, 'metadata.db')
    case 'win32':
      return path.join(process.env['APPDATA'] ?? path.join(homedir(), 'AppData', 'Roaming'), appName, 'metadata.db')
    default:
      // Linux / other — follow XDG Base Directory spec
      return path.join(process.env['XDG_DATA_HOME'] ?? path.join(homedir(), '.local', 'share'), appName, 'metadata.db')
  }
}
