/**
 * S3 route handlers — the core logic mapping S3 operations to FOC storage.
 *
 * Each handler receives Fastify request/reply + shared context (metadata store + synapse client).
 */

import { createHash } from 'node:crypto'
import type { FastifyInstance } from 'fastify'
import type { Logger } from 'pino'
import { sendInternalError, sendNoSuchBucket, sendNoSuchKey, sendS3Error } from '../s3/errors.js'
import { buildCopyObjectResultXml, buildListBucketsXml, buildListObjectsV2Xml } from '../s3/xml.js'
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
      // Get stored copy URLs for direct download (primary → secondary → SDK fallback)
      const copies = metadataStore.getObjectCopies(bucket, key)
      const data = await synapseClient.download(obj.pieceCid, copies)

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

  // ── PutObject / CopyObject: PUT /{bucket}/{key+} ──────────────────
  app.put('/:bucket/*', async (request, reply) => {
    const { bucket } = request.params as { bucket: string; '*': string }
    const key = (request.params as { '*': string })['*']

    if (!metadataStore.bucketExists(bucket)) {
      sendNoSuchBucket(reply, bucket)
      return
    }

    // ── CopyObject: detected by x-amz-copy-source header ───────────
    const copySource = request.headers['x-amz-copy-source'] as string | undefined
    if (copySource) {
      logger.debug({ bucket, key, copySource }, 'CopyObject')

      // Parse source: "/{bucket}/{key}" or "{bucket}/{key}"
      const normalized = copySource.startsWith('/') ? copySource.slice(1) : copySource
      const slashIdx = normalized.indexOf('/')
      if (slashIdx < 0) {
        sendS3Error(reply, 400, 'InvalidArgument', 'Invalid x-amz-copy-source format', copySource)
        return
      }
      const srcBucket = decodeURIComponent(normalized.slice(0, slashIdx))
      const srcKey = decodeURIComponent(normalized.slice(slashIdx + 1))

      if (!metadataStore.bucketExists(srcBucket)) {
        sendNoSuchBucket(reply, srcBucket)
        return
      }

      const copied = metadataStore.copyObject(srcBucket, srcKey, bucket, key)
      if (!copied) {
        sendNoSuchKey(reply, srcKey)
        return
      }

      const xml = buildCopyObjectResultXml(copied.etag, copied.lastModified)
      reply.header('Content-Type', 'application/xml').send(xml)
      return
    }

    // ── PutObject: regular upload ────────────────────────────────
    logger.debug({ bucket, key }, 'PutObject')

    try {
      const contentLength = request.headers['content-length']
        ? Number.parseInt(request.headers['content-length'] as string, 10)
        : undefined
      const contentType =
        (request.headers['content-type'] as string | undefined) ?? 'application/octet-stream'

      // Validate size from Content-Length header (fast path, no buffering)
      const MIN_UPLOAD_SIZE = 127
      const MAX_UPLOAD_SIZE = 1_065_353_216 // ~1 GiB with fr32 expansion

      if (contentLength !== undefined) {
        if (contentLength === 0) {
          // S3 allows empty objects, store metadata only
          const etag = createHash('md5').update('').digest('hex')
          metadataStore.putObject(bucket, key, '', 0, contentType, etag)
          reply.status(200).header('ETag', `"${etag}"`).send()
          return
        }
        if (contentLength < MIN_UPLOAD_SIZE) {
          sendS3Error(reply, 400, 'EntityTooSmall',
            `Object size ${contentLength} bytes is below minimum ${MIN_UPLOAD_SIZE} bytes required by Filecoin storage providers.`, key)
          return
        }
        if (contentLength > MAX_UPLOAD_SIZE) {
          sendS3Error(reply, 400, 'EntityTooLarge',
            `Object size ${contentLength} bytes exceeds maximum ${MAX_UPLOAD_SIZE} bytes (~1 GiB).`, key)
          return
        }
      }

      // Stream upload: convert Node.js Readable to Web ReadableStream
      // This avoids buffering the entire file in memory
      const md5 = createHash('md5')
      let totalBytes = 0

      const uploadStream = new ReadableStream<Uint8Array>({
        start(controller) {
          request.raw.on('data', (chunk: Buffer) => {
            const uint8 = new Uint8Array(chunk)
            md5.update(uint8)
            totalBytes += uint8.length
            controller.enqueue(uint8)
          })
          request.raw.on('end', () => {
            controller.close()
          })
          request.raw.on('error', (err) => {
            controller.error(err)
          })
        },
      })

      // Upload stream to FOC via Synapse SDK
      const result = await synapseClient.upload(uploadStream)
      const etag = md5.digest('hex')

      // Verify size after streaming (in case Content-Length was missing/wrong)
      if (totalBytes < MIN_UPLOAD_SIZE) {
        sendS3Error(reply, 400, 'EntityTooSmall',
          `Object size ${totalBytes} bytes is below minimum ${MIN_UPLOAD_SIZE} bytes required by Filecoin storage providers.`, key)
        return
      }

      // Store metadata
      metadataStore.putObject(bucket, key, result.pieceCid, result.size, contentType, etag, result.copies)

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
