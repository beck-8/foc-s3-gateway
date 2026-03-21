/**
 * S3 route handlers — the core logic mapping S3 operations to FOC storage.
 *
 * Each handler receives Fastify request/reply + shared context (metadata store + synapse client).
 */

import { createHash } from 'node:crypto'
import type { FastifyInstance } from 'fastify'
import type { Logger } from 'pino'
import { sendInternalError, sendNoSuchBucket, sendNoSuchKey, sendS3Error } from '../s3/errors.js'
import { buildListBucketsXml, buildListObjectsV2Xml } from '../s3/xml.js'
import type { MetadataStore } from '../storage/metadata-store.js'
import type { SynapseClient } from '../storage/synapse-client.js'

export interface RouteContext {
  metadataStore: MetadataStore
  synapseClient: SynapseClient
  logger: Logger
}

export function registerRoutes(app: FastifyInstance, ctx: RouteContext): void {
  const { metadataStore, synapseClient, logger } = ctx

  // ── ListBuckets: GET / ──────────────────────────────────────────────
  app.get('/', async (_request, reply) => {
    logger.debug('ListBuckets')

    const ownerId = synapseClient.getAddress()
    const buckets = metadataStore.listBuckets()
    const xml = buildListBucketsXml(buckets, ownerId)

    reply.header('Content-Type', 'application/xml').send(xml)
  })

  // ── CreateBucket: PUT /{bucket} (no wildcard key) ──────────────────
  app.put('/:bucket', async (request, reply) => {
    const { bucket } = request.params as { bucket: string }
    logger.debug({ bucket }, 'CreateBucket')

    if (metadataStore.bucketExists(bucket)) {
      // S3 returns 409 BucketAlreadyOwnedByYou — but many clients tolerate 200
      sendS3Error(reply, 409, 'BucketAlreadyOwnedByYou', 'Your previous request to create the named bucket succeeded and you already own it.', bucket)
      return
    }

    metadataStore.createBucket(bucket)
    reply.status(200).header('Location', `/${bucket}`).send()
  })

  // ── HeadBucket: HEAD /{bucket} ──────────────────────────────────────
  app.head('/:bucket', async (request, reply) => {
    const { bucket } = request.params as { bucket: string }
    logger.debug({ bucket }, 'HeadBucket')

    if (!metadataStore.bucketExists(bucket)) {
      sendNoSuchBucket(reply, bucket)
      return
    }

    reply.status(200).header('x-amz-bucket-region', 'us-east-1').send()
  })

  // ── DeleteBucket: DELETE /{bucket} (no wildcard key) ────────────────
  app.delete('/:bucket', async (request, reply) => {
    const { bucket } = request.params as { bucket: string }
    logger.debug({ bucket }, 'DeleteBucket')

    if (!metadataStore.bucketExists(bucket)) {
      sendNoSuchBucket(reply, bucket)
      return
    }

    const deleted = metadataStore.deleteBucket(bucket)
    if (!deleted) {
      sendS3Error(reply, 409, 'BucketNotEmpty', 'The bucket you tried to delete is not empty.', bucket)
      return
    }

    reply.status(204).send()
  })

  // ── ListObjectsV2: GET /{bucket}?list-type=2 ──────────────────────
  app.get('/:bucket', async (request, reply) => {
    const { bucket } = request.params as { bucket: string }
    const query = request.query as Record<string, string>
    logger.debug({ bucket, query }, 'ListObjectsV2')

    if (!metadataStore.bucketExists(bucket)) {
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

    const nextToken = isTruncated && lastKey ? lastKey : undefined

    const response = {
      name: bucket,
      prefix,
      maxKeys,
      isTruncated,
      contents: objects,
      commonPrefixes,
      keyCount: objects.length,
    }

    const xml = buildListObjectsV2Xml(
      nextToken ? { ...response, nextContinuationToken: nextToken } : response
    )

    reply.header('Content-Type', 'application/xml').send(xml)
  })

  // ── HeadObject: HEAD /{bucket}/{key+} ───────────────────────────────
  app.head('/:bucket/*', async (request, reply) => {
    const { bucket } = request.params as { bucket: string; '*': string }
    const key = (request.params as { '*': string })['*']
    logger.debug({ bucket, key }, 'HeadObject')

    if (!metadataStore.bucketExists(bucket)) {
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
  app.get('/:bucket/*', async (request, reply) => {
    const { bucket } = request.params as { bucket: string; '*': string }
    const key = (request.params as { '*': string })['*']
    logger.debug({ bucket, key }, 'GetObject')

    if (!metadataStore.bucketExists(bucket)) {
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
  app.put('/:bucket/*', async (request, reply) => {
    const { bucket } = request.params as { bucket: string; '*': string }
    const key = (request.params as { '*': string })['*']
    logger.debug({ bucket, key }, 'PutObject')

    if (!metadataStore.bucketExists(bucket)) {
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
  app.delete('/:bucket/*', async (request, reply) => {
    const { bucket } = request.params as { bucket: string; '*': string }
    const key = (request.params as { '*': string })['*']
    logger.debug({ bucket, key }, 'DeleteObject')

    if (!metadataStore.bucketExists(bucket)) {
      sendNoSuchBucket(reply, bucket)
      return
    }

    // S3 delete is idempotent — always returns 204
    metadataStore.deleteObject(bucket, key)

    reply.status(204).send()
  })
}
