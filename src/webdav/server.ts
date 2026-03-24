/**
 * WebDAV server — runs on a separate port from the S3 gateway.
 *
 * Uses Fastify with custom HTTP methods (PROPFIND, MKCOL, etc.)
 * registered via addHttpMethod (required for Fastify v5+).
 */

import Fastify from 'fastify'
import type { Logger } from 'pino'
import { createAuthHook } from '../auth/index.js'
import type { LocalStore } from '../storage/local-store.js'
import type { MetadataStore } from '../storage/metadata-store.js'
import type { SynapseClient } from '../storage/synapse-client.js'
import { registerWebDavRoutes } from './routes.js'

export interface WebDavServerOptions {
  port: number
  host: string
  metadataStore: MetadataStore
  synapseClient: SynapseClient
  localStore: LocalStore
  logger: Logger
  accessKey?: string | undefined
  secretKey?: string | undefined
}

export async function createWebDavServer(options: WebDavServerOptions) {
  const app = Fastify({
    logger: {
      level: 'info',
      transport: {
        target: 'pino-pretty',
        options: { colorize: true },
      },
      serializers: {
        // Display responseTime in seconds instead of milliseconds (same as S3 server)
        responseTime: (ms: number) => `${(ms / 1000).toFixed(2)}s`,
      },
    },
    bodyLimit: 1024 * 1024 * 1024, // 1 GiB
  })

  // Register WebDAV-specific HTTP methods (removed from Fastify v5 core)
  app.addHttpMethod('PROPFIND')
  app.addHttpMethod('PROPPATCH')
  app.addHttpMethod('MKCOL')
  app.addHttpMethod('COPY')
  app.addHttpMethod('MOVE')
  app.addHttpMethod('LOCK')
  app.addHttpMethod('UNLOCK')

  // Raw body handling
  app.removeAllContentTypeParsers()
  app.addContentTypeParser('*', (_request: any, _payload: any, done: any) => {
    done(null)
  })

  // Auth via Basic Auth
  if (options.accessKey && options.secretKey) {
    const authHook = createAuthHook({
      accessKey: options.accessKey,
      secretKey: options.secretKey,
      protocol: 'webdav',
      logger: app.log as Logger,
    })
    app.addHook('preHandler', authHook)
  }

  // Register routes
  registerWebDavRoutes(app, {
    metadataStore: options.metadataStore,
    synapseClient: options.synapseClient,
    localStore: options.localStore,
    logger: app.log as Logger,
  })

  return app
}

export async function startWebDavServer(options: WebDavServerOptions): Promise<void> {
  const app = await createWebDavServer(options)

  await app.listen({ port: options.port, host: options.host })
}
