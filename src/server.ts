/**
 * Fastify server that serves S3-compatible API over FOC storage.
 */

import { mkdirSync } from 'node:fs'
import { homedir } from 'node:os'
import path from 'node:path'
import Fastify from 'fastify'
import type { Logger } from 'pino'
import { registerRoutes } from './routes/index.js'
import { createAuthHook } from './auth/index.js'
import { MetadataStore } from './storage/metadata-store.js'
import { SynapseClient } from './storage/synapse-client.js'
import { startWebDavServer } from './webdav/server.js'

export interface ServerOptions {
  port: number
  host: string
  privateKey: string
  rpcUrl?: string | undefined
  network?: string | undefined
  dbPath?: string | undefined
  accessKey?: string | undefined
  secretKey?: string | undefined
  webdavPort?: number | undefined
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

  // Register auth if credentials provided
  if (options.accessKey && options.secretKey) {
    const authHook = createAuthHook({
      accessKey: options.accessKey,
      secretKey: options.secretKey,
      logger,
    })
    app.addHook('preHandler', authHook)
    logger.info('authentication enabled')
  } else {
    logger.warn('no access key / secret key configured — running WITHOUT authentication')
  }

  // Register S3 routes
  registerRoutes(app, { metadataStore, synapseClient, logger })

  // Graceful shutdown
  app.addHook('onClose', () => {
    metadataStore.close()
  })

  return { app, metadataStore, synapseClient }
}

export async function startServer(options: ServerOptions): Promise<void> {
  const { app, metadataStore, synapseClient } = await createServer(options)

  try {
    await app.listen({ port: options.port, host: options.host, listenTextResolver: () => '' })

    const address = app.server.address()
    const addressStr = typeof address === 'string' ? address : `${address?.address}:${address?.port}`

    // Start WebDAV server on a separate port
    const webdavPort = options.webdavPort ?? options.port + 1
    await startWebDavServer({
      port: webdavPort,
      host: options.host,
      metadataStore,
      synapseClient,
      logger: app.log as Logger,
      accessKey: options.accessKey,
      secretKey: options.secretKey,
    })

    const webdavAddr = `${options.host}:${webdavPort}`

    const authStatus = options.accessKey ? 'enabled' : 'disabled'
    app.log.info(`
  FOC S3 Gateway
  ----------------------------------
  S3:     http://${addressStr}
  WebDAV: http://${webdavAddr}
  Auth:   ${authStatus}
  ----------------------------------
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
