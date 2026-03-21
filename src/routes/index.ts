/**
 * S3 route handlers — the core logic mapping S3 operations to FOC storage.
 *
 * Each handler receives Fastify request/reply + shared context (metadata store + synapse client).
 */

import { createHash } from 'node:crypto'
import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify'
import type { Logger } from 'pino'
import { sendInternalError, sendNoSuchBucket, sendNoSuchKey } from '../s3/errors.js'
import { buildListBucketsXml, buildListObjectsV2Xml } from '../s3/xml.js'
import type { MetadataStore } from '../storage/metadata-store.js'
import type { SynapseClient } from '../storage/synapse-client.js'

const DEFAULT_BUCKET = 'default'

export interface RouteContext {
  metadataStore: MetadataStore
  synapseClient: SynapseClient
  logger: Logger
}

/** Parse bucket and key from S3-style path: /{bucket}/{key...} */
function parsePath(url: string): { bucket: string; key: string } {
  // Remove query string
  const pathOnly = url.split('?')[0] ?? url
  const parts = pathOnly.split('/').filter(Boolean)
  const bucket = parts[0] ?? ''
  const key = parts.slice(1).join('/')
  return { bucket, key }
}

/** Check if the bucket is valid (we only support the default bucket for now) */
function isValidBucket(bucket: string): boolean {
  return bucket === DEFAULT_BUCKET
}

export function registerRoutes(app: FastifyInstance, ctx: RouteContext): void {
  const { metadataStore, synapseClient, logger } = ctx

  // ── ListBuckets: GET / ──────────────────────────────────────────────
  app.get('/', async (_request, reply) => {
    logger.debug('ListBuckets')

    const ownerId = await synapseClient.getAddress()

    const xml = buildListBucketsXml(
      [
        {
          name: DEFAULT_BUCKET,
          creationDate: new Date().toISOString(),
        },
      ],
      ownerId
    )

    reply.header('Content-Type', 'application/xml').send(xml)
  })

  // ── HeadBucket: HEAD /{bucket} ──────────────────────────────────────
  app.head('/:bucket', async (request, reply) => {
    const { bucket } = request.params as { bucket: string }
    logger.debug({ bucket }, 'HeadBucket')

    if (!isValidBucket(bucket)) {
      sendNoSuchBucket(reply, bucket)
      return
    }

    reply.status(200).header('x-amz-bucket-region', 'us-east-1').send()
  })

  // ── ListObjectsV2: GET /{bucket}?list-type=2 ──────────────────────
  app.get('/:bucket', async (request, reply) => {
    const { bucket } = request.params as { bucket: string }
    const query = request.query as Record<string, string>
    logger.debug({ bucket, query }, 'ListObjectsV2')

    if (!isValidBucket(bucket)) {
      sendNoSuchBucket(reply, bucket)
      return
    }

    const prefix = query['prefix'] ?? ''
    const delimiter = query['delimiter'] ?? ''
    const maxKeys = Math.min(Number.parseInt(query['max-keys'] ?? '1000', 10), 1000)
    const startAfter = query['start-after'] ?? query['continuation-token']

    const { objects, commonPrefixes, isTruncated } = metadataStore.listObjects(
      bucket,
      prefix,
      delimiter,
      maxKeys,
      startAfter
    )

    const lastKey = objects[objects.length - 1]?.key

    const xml = buildListObjectsV2Xml({
      name: bucket,
      prefix,
      maxKeys,
      isTruncated,
      contents: objects,
      commonPrefixes,
      keyCount: objects.length,
      nextContinuationToken: isTruncated ? lastKey : undefined,
    })

    reply.header('Content-Type', 'application/xml').send(xml)
  })

  // ── HeadObject: HEAD /{bucket}/{key+} ───────────────────────────────
  app.head('/:bucket/*', async (request: FastifyRequest, reply: FastifyReply) => {
    const { bucket } = request.params as { bucket: string; '*': string }
    const key = (request.params as { '*': string })['*']
    logger.debug({ bucket, key }, 'HeadObject')

    if (!isValidBucket(bucket)) {
      sendNoSuchBucket(reply, bucket)
      return
    }

    const obj = metadataStore.getObject(bucket, key)
    if (!obj) {
      sendNoSuchKey(reply, key)
      return
    }

    reply
      .status(200)
      .header('Content-Type', obj.contentType)
      .header('Content-Length', obj.size)
      .header('ETag', `"${obj.etag}"`)
      .header('Last-Modified', new Date(obj.lastModified).toUTCString())
      .send()
  })

  // ── GetObject: GET /{bucket}/{key+} ─────────────────────────────────
  app.get('/:bucket/*', async (request: FastifyRequest, reply: FastifyReply) => {
    const { bucket } = request.params as { bucket: string; '*': string }
    const key = (request.params as { '*': string })['*']
    logger.debug({ bucket, key }, 'GetObject')

    if (!isValidBucket(bucket)) {
      sendNoSuchBucket(reply, bucket)
      return
    }

    const obj = metadataStore.getObject(bucket, key)
    if (!obj) {
      sendNoSuchKey(reply, key)
      return
    }

    try {
      const data = await synapseClient.download(obj.pieceCid)

      reply
        .status(200)
        .header('Content-Type', obj.contentType)
        .header('Content-Length', data.length)
        .header('ETag', `"${obj.etag}"`)
        .header('Last-Modified', new Date(obj.lastModified).toUTCString())
        .send(Buffer.from(data))
    } catch (error) {
      logger.error({ error, bucket, key, pieceCid: obj.pieceCid }, 'download failed')
      sendInternalError(reply, 'Failed to download object from FOC storage')
    }
  })

  // ── PutObject: PUT /{bucket}/{key+} ─────────────────────────────────
  app.put('/:bucket/*', async (request: FastifyRequest, reply: FastifyReply) => {
    const { bucket } = request.params as { bucket: string; '*': string }
    const key = (request.params as { '*': string })['*']
    logger.debug({ bucket, key }, 'PutObject')

    if (!isValidBucket(bucket)) {
      sendNoSuchBucket(reply, bucket)
      return
    }

    try {
      // Collect request body as buffer
      const chunks: Buffer[] = []
      for await (const chunk of request.raw) {
        chunks.push(Buffer.from(chunk))
      }
      const body = Buffer.concat(chunks)

      if (body.length === 0) {
        // S3 allows empty objects, store metadata only
        const etag = createHash('md5').update(body).digest('hex')
        const contentType =
          (request.headers['content-type'] as string | undefined) ?? 'application/octet-stream'
        metadataStore.putObject(bucket, key, '', 0, contentType, etag)

        reply
          .status(200)
          .header('ETag', `"${etag}"`)
          .send()
        return
      }

      // Upload to FOC via Synapse SDK
      const data = new Uint8Array(body)
      const result = await synapseClient.upload(data)

      // Store metadata
      const etag = createHash('md5').update(body).digest('hex')
      const contentType =
        (request.headers['content-type'] as string | undefined) ?? 'application/octet-stream'

      metadataStore.putObject(bucket, key, result.pieceCid, result.size, contentType, etag)

      reply
        .status(200)
        .header('ETag', `"${etag}"`)
        .send()
    } catch (error) {
      logger.error({ error, bucket, key }, 'upload failed')
      sendInternalError(reply, 'Failed to upload object to FOC storage')
    }
  })

  // ── DeleteObject: DELETE /{bucket}/{key+} ───────────────────────────
  app.delete('/:bucket/*', async (request: FastifyRequest, reply: FastifyReply) => {
    const { bucket } = request.params as { bucket: string; '*': string }
    const key = (request.params as { '*': string })['*']
    logger.debug({ bucket, key }, 'DeleteObject')

    if (!isValidBucket(bucket)) {
      sendNoSuchBucket(reply, bucket)
      return
    }

    // S3 delete is idempotent — always returns 204
    metadataStore.deleteObject(bucket, key)

    reply.status(204).send()
  })
}
