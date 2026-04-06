/**
 * WebDAV routes for FOC S3 Gateway.
 *
 * Runs on a dedicated Fastify instance on its own port.
 * Root (/) shows buckets as directories, /{bucket}/{key} maps to files.
 */

import { createHash, randomUUID } from 'node:crypto'
import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify'
import type { BlobFetcher } from 'foc-encryption'
import type { Logger } from 'pino'
import { parseRangeHeader } from '../s3/range.js'
import type { LocalStore } from '../storage/local-store.js'
import type { MetadataStore } from '../storage/metadata-store.js'
import type { EncryptionMeta, EncryptionService } from '../storage/encryption-service.js'
import type { SynapseClient } from '../storage/synapse-client.js'
import type { DavResource } from './xml.js'
import { buildMultistatusXml } from './xml.js'

export interface WebDavRouteOptions {
  metadataStore: MetadataStore
  synapseClient: SynapseClient
  localStore: LocalStore
  logger: Logger
  encryptionService?: EncryptionService | undefined
}

/** Parse a path into bucket and key components */
function parseDavPath(url: string): { bucket?: string; key?: string } {
  let path = url.split('?')[0] ?? ''
  path = decodeURIComponent(path)
  // Remove leading slash
  if (path.startsWith('/')) path = path.slice(1)
  // Remove trailing slash
  if (path.endsWith('/')) path = path.slice(0, -1)

  if (!path) return {} // root

  const slashIdx = path.indexOf('/')
  if (slashIdx < 0) return { bucket: path } // bucket level
  return { bucket: path.slice(0, slashIdx), key: path.slice(slashIdx + 1) } // file level
}

/** Get Depth header (0, 1, or infinity) */
function getDepth(request: FastifyRequest): number {
  const depth = request.headers.depth
  if (depth === '0') return 0
  if (depth === 'infinity') return Number.POSITIVE_INFINITY
  return 1 // default for '1', undefined, or any invalid value
}

function createSpBlobFetcher(retrievalUrl: string): BlobFetcher {
  return {
    async fetchEnvelope(): Promise<Uint8Array> {
      const resp = await fetch(retrievalUrl, { headers: { Range: 'bytes=0-4095' } })
      if (!resp.ok && resp.status !== 206) throw new Error(`Failed to fetch envelope: HTTP ${resp.status}`)
      return new Uint8Array(await resp.arrayBuffer())
    },
    async fetchRange(offset: number, length: number): Promise<Uint8Array> {
      const resp = await fetch(retrievalUrl, { headers: { Range: `bytes=${offset}-${offset + length - 1}` } })
      if (!resp.ok && resp.status !== 206) throw new Error(`Failed to fetch range: HTTP ${resp.status}`)
      return new Uint8Array(await resp.arrayBuffer())
    },
  }
}

export function registerWebDavRoutes(app: FastifyInstance, options: WebDavRouteOptions): void {
  const { metadataStore, synapseClient, localStore, logger, encryptionService } = options

  // ── OPTIONS ──────────────────────────────────────────────────────────
  app.options('/*', async (_request, reply) => {
    reply
      .status(200)
      .header('Allow', 'OPTIONS, GET, HEAD, PUT, DELETE, MKCOL, COPY, MOVE, PROPFIND')
      .header('DAV', '1')
      .header('Content-Length', '0')
      .send()
  })

  app.options('/', async (_request, reply) => {
    reply.status(200).header('Allow', 'OPTIONS, PROPFIND').header('DAV', '1').header('Content-Length', '0').send()
  })

  // ── PROPFIND ─────────────────────────────────────────────────────────
  app.route({
    method: 'PROPFIND',
    url: '/',
    handler: async (request, reply) => handlePropfind(request, reply, metadataStore, logger),
  })

  app.route({
    method: 'PROPFIND',
    url: '/*',
    handler: async (request, reply) => handlePropfind(request, reply, metadataStore, logger),
  })

  // ── GET (download) ───────────────────────────────────────────────────
  app.get('/*', async (request, reply) => {
    const { bucket, key } = parseDavPath(request.url)
    logger.debug({ bucket, key }, 'WebDAV GET')

    if (!bucket || !key) {
      reply.status(404).send('Not Found')
      return
    }

    if (!metadataStore.bucketExists(bucket)) {
      reply.status(404).send('Not Found')
      return
    }

    const obj = metadataStore.getObject(bucket, key)
    if (!obj) {
      reply.status(404).send('Not Found')
      return
    }

    // ── Range handling ────────────────────────────────────────────────
    const rangeHeaderValue = request.headers.range as string | undefined
    let range: { start: number; end: number } | undefined

    if (rangeHeaderValue) {
      const parsed = parseRangeHeader(rangeHeaderValue, obj.size)
      if (parsed === 'unsatisfiable') {
        reply.raw.writeHead(416, { 'Content-Range': `bytes */${obj.size}` })
        reply.raw.end('Range Not Satisfiable')
        return
      }
      if (parsed !== undefined) {
        range = parsed
      }
    }

    const baseHeaders: Record<string, string | number> = {
      'Content-Type': obj.contentType,
      ETag: `"${obj.etag}"`,
      'Last-Modified': new Date(obj.lastModified).toUTCString(),
      'Accept-Ranges': 'bytes',
    }

    try {
      // Local-first: try local disk before going to FOC
      const localPath = metadataStore.getLocalPath(bucket, key)
      if (localPath && localStore.exists(localPath)) {
        logger.debug({ bucket, key, localPath, range }, 'WebDAV serving from local disk')

        if (range) {
          const contentLength = range.end - range.start + 1
          const fileStream = localStore.createReadStream(localPath, { start: range.start, end: range.end })
          reply.raw.writeHead(206, {
            ...baseHeaders,
            'Content-Length': contentLength,
            'Content-Range': `bytes ${range.start}-${range.end}/${obj.size}`,
          })
          fileStream.pipe(reply.raw)
        } else {
          const fileStream = localStore.createReadStream(localPath)
          reply.raw.writeHead(200, {
            ...baseHeaders,
            'Content-Length': obj.size,
          })
          fileStream.pipe(reply.raw)
        }
        return
      }

      // Empty files (0B, no pieceCid) — return empty body with correct headers
      if (!obj.pieceCid || obj.size === 0) {
        reply.raw.writeHead(200, {
          ...baseHeaders,
          'Content-Length': 0,
        })
        reply.raw.end()
        return
      }

      // Fall back to FOC download
      const copies = metadataStore.getObjectCopies(bucket, key)
      const encMetaJson = encryptionService ? metadataStore.getEncryptionMeta(bucket, key) : null

      if (encMetaJson && encryptionService) {
        const { Readable } = await import('node:stream')
        const encMeta: EncryptionMeta = JSON.parse(encMetaJson)

        if (range && encMeta.algorithm === -65793) {
          // Seekable range decryption
          const primaryCopy = copies.find(c => c.role === 'primary') ?? copies[0]
          if (!primaryCopy) throw new Error('No copies available for range decryption')

          const fetcher = createSpBlobFetcher(primaryCopy.retrievalUrl)
          const metadata = await encryptionService.parseEnvelope(fetcher)
          const plainRange = await encryptionService.decryptRange(fetcher, metadata, {
            offset: range.start,
            length: range.end - range.start + 1,
          })

          reply.raw.writeHead(206, {
            ...baseHeaders,
            'Content-Length': plainRange.length,
            'Content-Range': `bytes ${range.start}-${range.end}/${obj.size}`,
          })
          Readable.from(plainRange).pipe(reply.raw)
        } else {
          // Full download + decrypt
          const encryptedBlob = await synapseClient.downloadBuffer(obj.pieceCid, copies)
          const plaintext = await encryptionService.decryptBuffer(encryptedBlob)

          if (range) {
            const sliced = plaintext.slice(range.start, range.end + 1)
            reply.raw.writeHead(206, {
              ...baseHeaders,
              'Content-Length': sliced.length,
              'Content-Range': `bytes ${range.start}-${range.end}/${obj.size}`,
            })
            const { Readable } = await import('node:stream')
            Readable.from(sliced).pipe(reply.raw)
          } else {
            reply.raw.writeHead(200, {
              ...baseHeaders,
              'Content-Length': obj.size,
            })
            const { Readable } = await import('node:stream')
            Readable.from(plaintext).pipe(reply.raw)
          }
        }
      } else {
        // Unencrypted path (existing behavior)
        const { stream } = await synapseClient.download(obj.pieceCid, copies, range)

        if (range) {
          const contentLength = range.end - range.start + 1
          reply.raw.writeHead(206, {
            ...baseHeaders,
            'Content-Length': contentLength,
            'Content-Range': `bytes ${range.start}-${range.end}/${obj.size}`,
          })
        } else {
          reply.raw.writeHead(200, {
            ...baseHeaders,
            'Content-Length': obj.size,
          })
        }
        stream.pipe(reply.raw)
      }
    } catch (error) {
      logger.error({ error, bucket, key }, 'WebDAV download failed')
      reply.status(500).send('Internal Server Error')
    }
  })

  // Note: HEAD is auto-generated by Fastify from GET routes

  // ── PUT (upload) ─────────────────────────────────────────────────────
  app.put('/*', async (request, reply) => {
    const { bucket, key } = parseDavPath(request.url)
    logger.debug({ bucket, key }, 'WebDAV PUT')

    if (!bucket || !key) {
      reply.status(409).send('Cannot write to root or bucket level')
      return
    }

    if (!metadataStore.bucketExists(bucket)) {
      reply.status(409).send('Bucket does not exist')
      return
    }

    try {
      const contentLength = request.headers['content-length']
        ? Number.parseInt(request.headers['content-length'] as string, 10)
        : undefined
      const contentType = (request.headers['content-type'] as string | undefined) ?? 'application/octet-stream'

      // Validate size from Content-Length header (fast path)
      const MIN_UPLOAD_SIZE = 127
      const MAX_UPLOAD_SIZE = 1_065_353_216

      if (contentLength !== undefined) {
        if (contentLength === 0) {
          const etag = createHash('md5').update('').digest('hex')
          metadataStore.putObject(bucket, key, '', 0, contentType, etag)
          reply.status(201).header('ETag', `"${etag}"`).send()
          return
        }
        if (contentLength < MIN_UPLOAD_SIZE) {
          reply.status(400).send(`File too small: ${contentLength} bytes (minimum ${MIN_UPLOAD_SIZE} bytes)`)
          return
        }
        if (contentLength > MAX_UPLOAD_SIZE) {
          reply.status(413).send(`File too large: ${contentLength} bytes (maximum ~1 GiB)`)
          return
        }
      }

      // Stage to local disk — this is fast (disk I/O only)
      const stageId = randomUUID()
      const staged = await localStore.stageUpload(stageId, request.raw)

      // Validate after streaming (Content-Length may have been missing)
      if (staged.size < MIN_UPLOAD_SIZE) {
        localStore.delete(staged.localPath)
        reply.status(400).send(`File too small: ${staged.size} bytes (minimum ${MIN_UPLOAD_SIZE} bytes)`)
        return
      }
      if (staged.size > MAX_UPLOAD_SIZE) {
        localStore.delete(staged.localPath)
        reply.status(413).send(`File too large: ${staged.size} bytes (maximum ~1 GiB)`)
        return
      }

      // Store metadata with status=pending, return immediately
      metadataStore.stageObject(bucket, key, staged.size, contentType, staged.etag, staged.localPath)

      reply.status(201).header('ETag', `"${staged.etag}"`).send()
    } catch (error) {
      logger.error({ error, bucket, key }, 'WebDAV upload failed')
      reply.status(500).send('Internal Server Error')
    }
  })

  // ── DELETE ───────────────────────────────────────────────────────────
  app.delete('/*', async (request, reply) => {
    const { bucket, key } = parseDavPath(request.url)
    logger.debug({ bucket, key }, 'WebDAV DELETE')

    if (!bucket) {
      reply.status(403).send('Cannot delete root')
      return
    }

    if (key) {
      if (!metadataStore.bucketExists(bucket)) {
        reply.status(404).send('Not Found')
        return
      }
      // Clean up local staged file before deleting metadata
      const localPath = metadataStore.getLocalPath(bucket, key)
      if (localPath) {
        localStore.delete(localPath)
      }
      metadataStore.deleteObject(bucket, key)
      reply.status(204).send()
      return
    }

    if (!metadataStore.bucketExists(bucket)) {
      reply.status(404).send('Not Found')
      return
    }
    const deleted = metadataStore.deleteBucket(bucket)
    if (!deleted) {
      reply.status(409).send('Bucket not empty or is protected')
      return
    }
    reply.status(204).send()
  })

  // ── MKCOL (create bucket) ───────────────────────────────────────────
  app.route({
    method: 'MKCOL',
    url: '/*',
    handler: async (request: FastifyRequest, reply: FastifyReply) => {
      const { bucket, key } = parseDavPath(request.url)
      logger.debug({ bucket, key }, 'WebDAV MKCOL')

      if (!bucket) {
        reply.status(403).send('Cannot create root')
        return
      }

      if (key) {
        reply.status(201).send()
        return
      }

      const created = metadataStore.createBucket(bucket)
      if (!created) {
        reply.status(405).send('Bucket already exists')
        return
      }
      reply.status(201).send()
    },
  })

  // ── COPY ────────────────────────────────────────────────────────────
  app.route({
    method: 'COPY',
    url: '/*',
    handler: async (request: FastifyRequest, reply: FastifyReply) => {
      const { bucket: srcBucket, key: srcKey } = parseDavPath(request.url)
      const destination = request.headers.destination as string | undefined

      logger.debug({ srcBucket, srcKey, destination }, 'WebDAV COPY')

      if (!srcBucket || !srcKey || !destination) {
        reply.status(400).send('Bad Request')
        return
      }

      let destUrl: URL
      try {
        destUrl = new URL(destination, `http://${request.headers.host}`)
      } catch {
        reply.status(400).send('Invalid destination URL')
        return
      }
      const { bucket: dstBucket, key: dstKey } = parseDavPath(destUrl.pathname)

      if (!dstBucket || !dstKey) {
        reply.status(400).send('Invalid destination')
        return
      }

      if (!metadataStore.bucketExists(dstBucket)) {
        reply.status(409).send('Destination bucket does not exist')
        return
      }

      const copied = metadataStore.copyObject(srcBucket, srcKey, dstBucket, dstKey)
      if (!copied) {
        reply.status(404).send('Source not found')
        return
      }

      reply.status(201).send()
    },
  })

  // ── MOVE ────────────────────────────────────────────────────────────
  app.route({
    method: 'MOVE',
    url: '/*',
    handler: async (request: FastifyRequest, reply: FastifyReply) => {
      const { bucket: srcBucket, key: srcKey } = parseDavPath(request.url)
      const destination = request.headers.destination as string | undefined

      logger.debug({ srcBucket, srcKey, destination }, 'WebDAV MOVE')

      if (!srcBucket || !srcKey || !destination) {
        reply.status(400).send('Bad Request')
        return
      }

      let destUrl: URL
      try {
        destUrl = new URL(destination, `http://${request.headers.host}`)
      } catch {
        reply.status(400).send('Invalid destination URL')
        return
      }
      const { bucket: dstBucket, key: dstKey } = parseDavPath(destUrl.pathname)

      if (!dstBucket || !dstKey) {
        reply.status(400).send('Invalid destination')
        return
      }

      if (!metadataStore.bucketExists(dstBucket)) {
        reply.status(409).send('Destination bucket does not exist')
        return
      }

      const copied = metadataStore.copyObject(srcBucket, srcKey, dstBucket, dstKey)
      if (!copied) {
        reply.status(404).send('Source not found')
        return
      }

      metadataStore.deleteObject(srcBucket, srcKey)
      reply.status(201).send()
    },
  })

  // ── LOCK (stub) ─────────────────────────────────────────────────
  app.route({
    method: 'LOCK',
    url: '/*',
    handler: async (_request: FastifyRequest, reply: FastifyReply) => {
      const token = `opaquelocktoken:${Math.random().toString(36).substring(2)}`
      reply
        .status(200)
        .header('Lock-Token', `<${token}>`)
        .header('Content-Type', 'application/xml; charset=utf-8')
        .send(`<?xml version="1.0" encoding="UTF-8"?>
<D:prop xmlns:D="DAV:">
  <D:lockdiscovery>
    <D:activelock>
      <D:locktype><D:write/></D:locktype>
      <D:lockscope><D:exclusive/></D:lockscope>
      <D:depth>infinity</D:depth>
      <D:timeout>Second-3600</D:timeout>
      <D:locktoken><D:href>${token}</D:href></D:locktoken>
    </D:activelock>
  </D:lockdiscovery>
</D:prop>`)
    },
  })

  // ── UNLOCK (stub) ──────────────────────────────────────────────
  app.route({
    method: 'UNLOCK',
    url: '/*',
    handler: async (_request: FastifyRequest, reply: FastifyReply) => {
      reply.status(204).send()
    },
  })

  // ── PROPPATCH (stub) ──────────────────────────────────────────
  app.route({
    method: 'PROPPATCH',
    url: '/*',
    handler: async (request: FastifyRequest, reply: FastifyReply) => {
      const { bucket, key } = parseDavPath(request.url)
      const href = key ? `/${bucket}/${key}` : `/${bucket ?? ''}`

      reply
        .status(207)
        .header('Content-Type', 'application/xml; charset=utf-8')
        .send(`<?xml version="1.0" encoding="UTF-8"?>
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>${href}</D:href>
    <D:propstat>
      <D:prop/>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>`)
    },
  })
}

// ── PROPFIND handler ───────────────────────────────────────────────────

async function handlePropfind(
  request: FastifyRequest,
  reply: FastifyReply,
  metadataStore: MetadataStore,
  logger: Logger
): Promise<void> {
  const { bucket, key } = parseDavPath(request.url)
  const depth = getDepth(request)

  logger.debug({ bucket, key, depth }, 'WebDAV PROPFIND')

  const resources: DavResource[] = []

  if (!bucket) {
    // Root: list buckets as collections
    resources.push({ href: '/', displayName: '', isCollection: true })

    if (depth > 0) {
      const buckets = metadataStore.listBuckets()
      for (const b of buckets) {
        resources.push({
          href: `/${b.name}/`,
          displayName: b.name,
          isCollection: true,
          lastModified: b.creationDate,
        })
      }
    }
  } else if (key) {
    // File or sub-directory level
    if (!metadataStore.bucketExists(bucket)) {
      reply.status(404).send('Not Found')
      return
    }

    const obj = metadataStore.getObject(bucket, key)
    if (obj) {
      resources.push({
        href: `/${bucket}/${key}`,
        displayName: key.split('/').pop() ?? key,
        isCollection: false,
        contentLength: obj.size,
        contentType: obj.contentType,
        lastModified: obj.lastModified,
        etag: obj.etag,
      })
    } else {
      const prefix = key.endsWith('/') ? key : `${key}/`
      const { objects, commonPrefixes } = metadataStore.listObjects(bucket, prefix, '/', 10000)

      if (objects.length === 0 && commonPrefixes.length === 0) {
        reply.status(404).send('Not Found')
        return
      }

      resources.push({
        href: `/${bucket}/${prefix}`,
        displayName: key.split('/').pop() ?? key,
        isCollection: true,
      })

      if (depth > 0) {
        for (const cp of commonPrefixes) {
          resources.push({
            href: `/${bucket}/${cp}`,
            displayName: cp.replace(/\/$/, '').split('/').pop() ?? cp,
            isCollection: true,
          })
        }
        for (const o of objects) {
          resources.push({
            href: `/${bucket}/${o.key}`,
            displayName: o.key.split('/').pop() ?? o.key,
            isCollection: false,
            contentLength: o.size,
            contentType: o.contentType,
            lastModified: o.lastModified,
            etag: o.etag,
          })
        }
      }
    }
  } else {
    // Bucket level
    if (!metadataStore.bucketExists(bucket)) {
      reply.status(404).send('Not Found')
      return
    }

    resources.push({ href: `/${bucket}/`, displayName: bucket, isCollection: true })

    if (depth > 0) {
      const { objects, commonPrefixes } = metadataStore.listObjects(bucket, '', '/', 10000)

      for (const prefix of commonPrefixes) {
        resources.push({
          href: `/${bucket}/${prefix}`,
          displayName: prefix.replace(/\/$/, '').split('/').pop() ?? prefix,
          isCollection: true,
        })
      }

      for (const obj of objects) {
        resources.push({
          href: `/${bucket}/${obj.key}`,
          displayName: obj.key.split('/').pop() ?? obj.key,
          isCollection: false,
          contentLength: obj.size,
          contentType: obj.contentType,
          lastModified: obj.lastModified,
          etag: obj.etag,
        })
      }
    }
  }

  const xml = buildMultistatusXml(resources)
  reply.status(207).header('Content-Type', 'application/xml; charset=utf-8').send(xml)
}
