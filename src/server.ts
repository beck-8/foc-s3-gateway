/**
 * Fastify server that serves S3-compatible API over FOC storage.
 */

import { mkdirSync } from 'node:fs'
import { homedir } from 'node:os'
import path from 'node:path'
import Fastify from 'fastify'
import type { Logger } from 'pino'
import { createAuthHook } from './auth/index.js'
import { registerRoutes } from './routes/index.js'
import { CleanupWorker } from './storage/cleanup-worker.js'
import { LocalStore } from './storage/local-store.js'
import { MetadataStore } from './storage/metadata-store.js'
import { SynapseClient } from './storage/synapse-client.js'
import { UploadWorker } from './storage/upload-worker.js'
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
      serializers: {
        // Display responseTime in seconds instead of milliseconds
        responseTime: (ms: number) => `${(ms / 1000).toFixed(2)}s`,
      },
    },
    // Disable body parsing — we handle raw streams for PutObject
    bodyLimit: 1024 * 1024 * 1024, // 1 GiB
  })

  const logger = app.log as Logger

  // Resolve database path — use platform-specific data dir by default
  const dbPath = options.dbPath ?? getDefaultDbPath()
  const dataDir = path.dirname(dbPath)
  mkdirSync(dataDir, { recursive: true })
  logger.info({ dbPath }, 'using database')

  // Initialize storage
  const metadataStore = new MetadataStore({ dbPath, logger })
  const synapseClient = new SynapseClient({
    privateKey: options.privateKey,
    rpcUrl: options.rpcUrl,
    network: options.network,
    logger,
  })
  const localStore = new LocalStore({ dataDir, logger })

  // Validate wallet address — ensures the PRIVATE_KEY matches the one
  // used when this database was first created. Prevents accidental key changes
  // that would break uploads, downloads, and deletions.
  const walletAddress = synapseClient.getAddress()
  metadataStore.validateWalletAddress(walletAddress)

  // Parse XML bodies as strings (needed for DeleteObjects, etc.)
  // All other content types pass through as raw streams (for PutObject uploads)
  app.removeAllContentTypeParsers()
  app.addContentTypeParser('application/xml', { parseAs: 'string' }, (_request, body, done) => {
    done(null, body)
  })
  app.addContentTypeParser('text/xml', { parseAs: 'string' }, (_request, body, done) => {
    done(null, body)
  })
  app.addContentTypeParser('*', (_request, _payload, done) => {
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
    logger.warn('no access key / secret key configured -- running WITHOUT authentication')
  }

  // Register S3 routes
  registerRoutes(app, { metadataStore, synapseClient, localStore, logger })

  const cleanupWorker = new CleanupWorker({ metadataStore, synapseClient, logger })
  const uploadWorker = new UploadWorker({ metadataStore, synapseClient, localStore, logger })

  // Clean up orphaned staging files on startup
  const knownPaths = metadataStore.getAllLocalPaths()
  localStore.cleanupOrphans(knownPaths)

  // Graceful shutdown
  app.addHook('onClose', () => {
    uploadWorker.stop()
    cleanupWorker.stop()
    metadataStore.close()
  })

  return { app, metadataStore, synapseClient, localStore, cleanupWorker, uploadWorker }
}

export async function startServer(options: ServerOptions): Promise<void> {
  const { app, metadataStore, synapseClient, localStore, cleanupWorker, uploadWorker } = await createServer(options)

  try {
    cleanupWorker.start()
    uploadWorker.start()
    await app.listen({ port: options.port, host: options.host })

    const address = app.server.address()
    const addressStr = typeof address === 'string' ? address : `${address?.address}:${address?.port}`

    // Start WebDAV server on a separate port
    const webdavPort = options.webdavPort ?? options.port + 1
    await startWebDavServer({
      port: webdavPort,
      host: options.host,
      metadataStore,
      synapseClient,
      localStore,
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
