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
import { EncryptionService } from './storage/encryption-service.js'
import { CleanupWorker } from './storage/cleanup-worker.js'
import { LocalStore } from './storage/local-store.js'
import { MetadataStore } from './storage/metadata-store.js'
import { ProbeWorker, type ProbeWorkerOptions } from './storage/probe-worker.js'
import { RepairWorker, type RepairWorkerOptions } from './storage/repair-worker.js'
import { SynapseClient } from './storage/synapse-client.js'
import { UploadWorker, type UploadWorkerOptions } from './storage/upload-worker.js'
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
  copies?: number | undefined
  scanIntervalMs?: number | undefined
  uploadConcurrency?: number | undefined
  probeScanIntervalMs?: number | undefined
  probeConcurrency?: number | undefined
  probeIntervalMs?: number | undefined
  probeTimeoutMs?: number | undefined
  unhealthyFailureThreshold?: number | undefined
  repairScanIntervalMs?: number | undefined
  repairConcurrency?: number | undefined
  repairCooldownMs?: number | undefined
  /** Enable client-side encryption. Requires secretKey to be set. */
  encryption?: boolean | undefined
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
  const desiredCopies = options.copies ?? 2
  metadataStore.setDefaultDesiredCopies(desiredCopies)
  logger.info({ desiredCopies }, 'configured default desired copies for new uploads')
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

  // Initialize encryption if enabled and secret key is available
  let encryptionService: EncryptionService | undefined
  if (options.encryption && options.secretKey) {
    const existingSaltHex = metadataStore.getConfig('encryption_salt')
    const existingSalt = existingSaltHex ? new Uint8Array(Buffer.from(existingSaltHex, 'hex')) : undefined

    encryptionService = new EncryptionService({
      secretKey: options.secretKey,
      ...(existingSalt !== undefined ? { salt: existingSalt } : {}),
    })
    await encryptionService.init()

    // Persist salt on first run so the same CEK is derived on restart
    if (!existingSaltHex) {
      const salt = encryptionService.getSalt()
      metadataStore.setConfig('encryption_salt', Buffer.from(salt).toString('hex'))
      logger.info('encryption salt generated and stored')
    }

    logger.info('client-side encryption enabled (AES-256-GCM, key derived from Secret Key)')
  } else if (options.encryption && !options.secretKey) {
    throw new Error('Encryption requires --secret-key (or SECRET_KEY env) to derive the encryption key')
  }

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

  const cleanupWorker = new CleanupWorker({ metadataStore, synapseClient, logger })
  const uploadWorkerOptions: UploadWorkerOptions = {
    metadataStore,
    synapseClient,
    localStore,
    logger,
    encryptionService,
  }
  const scanIntervalMs = options.scanIntervalMs ?? readPositiveIntEnv('UPLOAD_SCAN_INTERVAL_MS')
  const uploadConcurrency = options.uploadConcurrency ?? readPositiveIntEnv('UPLOAD_CONCURRENCY')
  const probeScanIntervalMs = options.probeScanIntervalMs ?? readPositiveIntEnv('PROBE_SCAN_INTERVAL_MS')
  const probeConcurrency = options.probeConcurrency ?? readPositiveIntEnv('PROBE_CONCURRENCY')
  const probeIntervalMs = options.probeIntervalMs ?? readPositiveIntEnv('COPY_PROBE_INTERVAL_MS')
  const probeTimeoutMs = options.probeTimeoutMs ?? readPositiveIntEnv('COPY_PROBE_TIMEOUT_MS')
  const unhealthyFailureThreshold =
    options.unhealthyFailureThreshold ?? readPositiveIntEnv('COPY_UNHEALTHY_FAILURE_THRESHOLD')
  const repairScanIntervalMs = options.repairScanIntervalMs ?? readPositiveIntEnv('REPAIR_SCAN_INTERVAL_MS')
  const repairConcurrency = options.repairConcurrency ?? readPositiveIntEnv('REPAIR_CONCURRENCY')
  const repairCooldownMs = options.repairCooldownMs ?? readPositiveIntEnv('REPAIR_COOLDOWN_MS')

  if (scanIntervalMs !== undefined) uploadWorkerOptions.intervalMs = scanIntervalMs
  if (uploadConcurrency !== undefined) uploadWorkerOptions.concurrency = uploadConcurrency

  const uploadWorker = new UploadWorker(uploadWorkerOptions)
  const probeWorkerOptions: ProbeWorkerOptions = {
    metadataStore,
    synapseClient,
    logger,
  }
  if (probeScanIntervalMs !== undefined) probeWorkerOptions.scanIntervalMs = probeScanIntervalMs
  if (probeConcurrency !== undefined) probeWorkerOptions.concurrency = probeConcurrency
  if (probeIntervalMs !== undefined) probeWorkerOptions.probeIntervalMs = probeIntervalMs
  if (probeTimeoutMs !== undefined) probeWorkerOptions.probeTimeoutMs = probeTimeoutMs
  if (unhealthyFailureThreshold !== undefined) {
    probeWorkerOptions.unhealthyFailureThreshold = unhealthyFailureThreshold
  }
  const probeWorker = new ProbeWorker(probeWorkerOptions)

  const repairWorkerOptions: RepairWorkerOptions = {
    metadataStore,
    synapseClient,
    logger,
  }
  if (repairScanIntervalMs !== undefined) repairWorkerOptions.scanIntervalMs = repairScanIntervalMs
  if (repairConcurrency !== undefined) repairWorkerOptions.concurrency = repairConcurrency
  if (repairCooldownMs !== undefined) repairWorkerOptions.cooldownMs = repairCooldownMs
  const repairWorker = new RepairWorker(repairWorkerOptions)

  // Register S3 routes
  registerRoutes(app, { metadataStore, synapseClient, localStore, uploadWorker, probeWorker, repairWorker, encryptionService, logger })

  // Reset objects stuck in 'uploading' state from a previous server run
  metadataStore.resetStuckUploads()

  // Clean up orphaned staging files on startup
  const knownPaths = metadataStore.getAllLocalPaths()
  localStore.cleanupOrphans(knownPaths)

  // Graceful shutdown
  app.addHook('onClose', () => {
    uploadWorker.stop()
    probeWorker.stop()
    repairWorker.stop()
    cleanupWorker.stop()
    metadataStore.close()
  })

  return { app, metadataStore, synapseClient, localStore, cleanupWorker, uploadWorker, probeWorker, repairWorker, encryptionService }
}

export async function startServer(options: ServerOptions): Promise<void> {
  const { app, metadataStore, synapseClient, localStore, cleanupWorker, uploadWorker, probeWorker, repairWorker, encryptionService } =
    await createServer(options)

  try {
    cleanupWorker.start()
    uploadWorker.start()
    probeWorker.start()
    repairWorker.start()
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
      encryptionService,
    })

    const webdavAddr = `${options.host}:${webdavPort}`

    const authStatus = options.accessKey ? 'enabled' : 'disabled'
    const encStatus = encryptionService ? 'enabled (AES-256-GCM)' : 'disabled'
    app.log.info(`
  FOC S3 Gateway
  ----------------------------------
  S3:         http://${addressStr}
  WebDAV:     http://${webdavAddr}
  Auth:       ${authStatus}
  Encryption: ${encStatus}
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
      return path.join(process.env.APPDATA ?? path.join(homedir(), 'AppData', 'Roaming'), appName, 'metadata.db')
    default:
      // Linux / other — follow XDG Base Directory spec
      return path.join(process.env.XDG_DATA_HOME ?? path.join(homedir(), '.local', 'share'), appName, 'metadata.db')
  }
}

function readPositiveIntEnv(name: string): number | undefined {
  const raw = process.env[name]
  if (raw === undefined || raw === '') return undefined

  const parsed = Number.parseInt(raw, 10)
  if (!Number.isInteger(parsed) || parsed < 1) {
    throw new Error(`${name} must be an integer >= 1 (received: ${raw})`)
  }
  return parsed
}
