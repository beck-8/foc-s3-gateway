/**
 * S3 route handlers — the core logic mapping S3 operations to FOC storage.
 *
 * Each handler receives Fastify request/reply + shared context (metadata store + synapse client).
 *
 * Upload flow (async):
 *   PutObject → save to local disk → return 200 immediately → UploadWorker sends to FOC later
 *
 * Multipart upload flow:
 *   InitiateMultipartUpload → UploadPart (×N) → CompleteMultipartUpload → merge on disk → async FOC upload
 *
 * Download flow (local-first):
 *   GetObject → try local disk → try SP direct URLs → fall back to SDK discovery
 */

import { createHash, randomUUID } from 'node:crypto'
import type { FastifyInstance } from 'fastify'
import type { Logger } from 'pino'
import { sendInternalError, sendInvalidRange, sendNoSuchBucket, sendNoSuchKey, sendS3Error } from '../s3/errors.js'
import { parseRangeHeader } from '../s3/range.js'
import {
  buildCompleteMultipartUploadXml,
  buildCopyObjectResultXml,
  buildDeleteResultXml,
  buildInitiateMultipartUploadXml,
  buildListBucketsXml,
  buildListObjectsV2Xml,
} from '../s3/xml.js'
import type { EncryptionService } from '../storage/encryption-service.js'
import type { LocalStore } from '../storage/local-store.js'
import type { MetadataStore } from '../storage/metadata-store.js'
import type { ProbeWorker } from '../storage/probe-worker.js'
import type { RepairWorker } from '../storage/repair-worker.js'
import type { SynapseClient } from '../storage/synapse-client.js'
import type { UploadWorker } from '../storage/upload-worker.js'

export interface RouteContext {
  metadataStore: MetadataStore
  synapseClient: SynapseClient
  localStore: LocalStore
  uploadWorker?: UploadWorker | undefined
  probeWorker?: ProbeWorker | undefined
  repairWorker?: RepairWorker | undefined
  encryptionService?: EncryptionService | undefined
  logger: Logger
}

/** Minimum file size for FOC storage providers */
const MIN_UPLOAD_SIZE = 127
/** Maximum upload size (~1 GiB) */
const MAX_UPLOAD_SIZE = 1_065_353_216

/** Read XML body from request — handles cases where Content-Type isn't application/xml */
async function readXmlBody(request: import('fastify').FastifyRequest): Promise<string> {
  // If Fastify parsed the body (Content-Type matched xml parser), use it
  if (typeof request.body === 'string' && request.body.length > 0) {
    return request.body
  }
  // Otherwise read from raw stream (Content-Type didn't match, body was discarded)
  return new Promise<string>((resolve, reject) => {
    const chunks: Buffer[] = []
    request.raw.on('data', (chunk: Buffer) => chunks.push(chunk))
    request.raw.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')))
    request.raw.on('error', reject)
  })
}

/** Format bytes to human-readable string (e.g. "1.5 GB") */
function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B'
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  const k = 1024
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  const value = bytes / k ** i
  return `${value < 10 ? value.toFixed(2) : value < 100 ? value.toFixed(1) : Math.round(value)} ${units[i]}`
}

/** Add a human-friendly `sizeFormatted` field to a status item */
function addFormattedSize<T extends { size: number }>(item: T): T & { sizeFormatted: string } {
  return { ...item, sizeFormatted: formatBytes(item.size) }
}

function parseCopySource(copySource: string): { bucket: string; key: string } | undefined {
  let normalized = copySource

  try {
    if (copySource.startsWith('http://') || copySource.startsWith('https://')) {
      normalized = new URL(copySource).pathname
    } else {
      normalized = copySource.split('?')[0] ?? copySource
    }
  } catch {
    normalized = copySource.split('?')[0] ?? copySource
  }

  if (normalized.startsWith('/')) {
    normalized = normalized.slice(1)
  }

  let slashIdx = normalized.indexOf('/')
  if (slashIdx < 0) {
    // Some clients encode the full source path (e.g. "bucket%2Fkey")
    try {
      normalized = decodeURIComponent(normalized)
      slashIdx = normalized.indexOf('/')
    } catch {
      return undefined
    }
  }
  if (slashIdx < 0) {
    return undefined
  }

  const bucket = decodeURIComponent(normalized.slice(0, slashIdx))
  if (bucket.includes('/')) return undefined
  return {
    bucket,
    key: decodeURIComponent(normalized.slice(slashIdx + 1)),
  }
}

export function registerRoutes(app: FastifyInstance, ctx: RouteContext): void {
  const { metadataStore, synapseClient, localStore, uploadWorker, probeWorker, repairWorker, encryptionService, logger } = ctx

  // ── Upload status: GET /_/status ────────────────────────────────────
  //    Not an S3 API — gateway-specific endpoint for monitoring upload queue.
  app.get('/_/status', async (request, reply) => {
    const query = request.query as Record<string, string>
    const stats = metadataStore.getUploadStats()
    const diskStats = localStore.getDiskStats()
    const multipartCount = metadataStore.countAllMultipartUploads()
    const deletionStats = metadataStore.getDeletionStats()
    const objectSummary = metadataStore.getObjectSummary()
    const copyHealth = metadataStore.getCopyHealthSummary()
    const uploadStatus = uploadWorker?.getStatus()
    const probeStatus = probeWorker?.getStatus()
    const repairStatus = repairWorker?.getStatus()

    const result: Record<string, unknown> = {
      objects: {
        totalFiles: objectSummary.totalFiles,
        totalBytes: objectSummary.totalBytes,
        totalSize: formatBytes(objectSummary.totalBytes),
      },
      replication: {
        eligibleFiles: copyHealth.eligibleFiles,
        healthyFiles: copyHealth.healthyFiles,
        suspectFiles: copyHealth.suspectFiles,
        unhealthyFiles: copyHealth.unhealthyFiles,
        failedFiles: copyHealth.failedFiles,
        emptyFiles: objectSummary.emptyFiles,
        repairingFiles: repairStatus?.inProgress ?? 0,
        coolingDownFiles: repairStatus?.coolingDown ?? 0,
      },
      uploads: stats,
      deletions: deletionStats,
      disk: {
        staging: {
          files: diskStats.staging.count,
          totalBytes: diskStats.staging.totalBytes,
          totalSize: formatBytes(diskStats.staging.totalBytes),
        },
        multipartParts: {
          files: diskStats.multipart.count,
          totalBytes: diskStats.multipart.totalBytes,
          totalSize: formatBytes(diskStats.multipart.totalBytes),
        },
      },
      multipartUploads: multipartCount,
    }

    if (uploadStatus) {
      result.uploadWorker = uploadStatus
    }
    if (probeStatus) {
      result.probe = probeStatus
    }
    if (repairStatus) {
      result.repair = repairStatus
    }

    // ?detail=true → include object lists for pending/uploading/failed
    if (query.detail === 'true') {
      result.pending = metadataStore.getObjectsByStatus('pending').map(addFormattedSize)
      result.uploading = metadataStore.getObjectsByStatus('uploading').map(addFormattedSize)
      result.failed = metadataStore.getObjectsByStatus('failed').map(addFormattedSize)
    }

    reply.header('Content-Type', 'application/json').send(result)
  })

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
      sendS3Error(
        reply,
        409,
        'BucketAlreadyOwnedByYou',
        'Your previous request to create the named bucket succeeded and you already own it.',
        bucket
      )
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

  // ── ListObjectsV2 / GetBucketLocation / GetBucketVersioning: GET /{bucket}
  app.get('/:bucket', async (request, reply) => {
    const { bucket } = request.params as { bucket: string }
    const query = request.query as Record<string, string>

    if (!metadataStore.bucketExists(bucket)) {
      sendNoSuchBucket(reply, bucket)
      return
    }

    // GetBucketLocation: GET /bucket?location (used by mc to determine signing region)
    if ('location' in query) {
      logger.debug({ bucket }, 'GetBucketLocation')
      reply.header('Content-Type', 'application/xml').send(`<?xml version="1.0" encoding="UTF-8"?>
<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>`)
      return
    }

    // GetBucketVersioning: GET /bucket?versioning (many S3 clients check this)
    if ('versioning' in query) {
      logger.debug({ bucket }, 'GetBucketVersioning')
      reply.header('Content-Type', 'application/xml').send(`<?xml version="1.0" encoding="UTF-8"?>
<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>`)
      return
    }

    logger.debug({ bucket, query }, 'ListObjectsV2')

    const prefix = query.prefix ?? ''
    const delimiter = query.delimiter ?? ''
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

    const xml = buildListObjectsV2Xml(nextToken ? { ...response, nextContinuationToken: nextToken } : response)

    reply.header('Content-Type', 'application/xml').send(xml)
  })

  // ── HeadObject: HEAD /{bucket}/{key+} ───────────────────────────────
  app.head('/:bucket/*', async (request, reply) => {
    const { bucket } = request.params as { bucket: string; '*': string }
    const key = (request.params as { '*': string })['*']

    // Trailing slash: HEAD /bucket/ → treat as HeadBucket
    if (!key) {
      logger.debug({ bucket }, 'HeadBucket (trailing slash)')
      if (!metadataStore.bucketExists(bucket)) {
        sendNoSuchBucket(reply, bucket)
        return
      }
      reply.status(200).header('x-amz-bucket-region', 'us-east-1').send()
      return
    }

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
      .header('Accept-Ranges', 'bytes')
      .send()
  })

  // ── GetObject: GET /{bucket}/{key+} ─────────────────────────────────
  //    When key is empty (e.g. GET /bucket/ with trailing slash), treat as ListObjectsV2.
  app.get('/:bucket/*', async (request, reply) => {
    const { bucket } = request.params as { bucket: string; '*': string }
    const key = (request.params as { '*': string })['*']

    // Trailing-slash requests (GET /bucket/) have empty key — redirect to bucket-level GET
    if (!key) {
      const query = request.query as Record<string, string>

      if (!metadataStore.bucketExists(bucket)) {
        sendNoSuchBucket(reply, bucket)
        return
      }

      // GetBucketLocation: GET /bucket/?location
      if ('location' in query) {
        logger.debug({ bucket }, 'GetBucketLocation (trailing slash)')
        reply.header('Content-Type', 'application/xml').send(`<?xml version="1.0" encoding="UTF-8"?>
<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>`)
        return
      }

      // GetBucketVersioning: GET /bucket/?versioning
      if ('versioning' in query) {
        logger.debug({ bucket }, 'GetBucketVersioning (trailing slash)')
        reply.header('Content-Type', 'application/xml').send(`<?xml version="1.0" encoding="UTF-8"?>
<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>`)
        return
      }

      logger.debug({ bucket, query }, 'ListObjectsV2 (trailing slash)')

      const prefix = query.prefix ?? ''
      const delimiter = query.delimiter ?? ''
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
      const xml = buildListObjectsV2Xml(nextToken ? { ...response, nextContinuationToken: nextToken } : response)
      reply.header('Content-Type', 'application/xml').send(xml)
      return
    }

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

    // ── Range handling ────────────────────────────────────────────────
    const rangeHeader = request.headers.range as string | undefined
    let range: { start: number; end: number } | undefined

    if (rangeHeader) {
      const parsed = parseRangeHeader(rangeHeader, obj.size)
      if (parsed === 'unsatisfiable') {
        sendInvalidRange(reply, obj.size, key)
        return
      }
      // undefined = unparseable / non-byte range → ignore, serve full 200
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
        logger.debug({ bucket, key, localPath, range }, 'serving from local disk')

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
        reply
          .status(200)
          .header('Content-Type', obj.contentType)
          .header('Content-Length', '0')
          .header('ETag', `"${obj.etag}"`)
          .header('Last-Modified', new Date(obj.lastModified).toUTCString())
          .header('Accept-Ranges', 'bytes')
          .send('')
        return
      }

      // Fall back to FOC download (direct SP URLs → SDK discovery)
      const copies = metadataStore.getObjectCopies(bucket, key)
      const encMetaJson = encryptionService ? metadataStore.getEncryptionMeta(bucket, key) : null

      if (encMetaJson && encryptionService) {
        // Encrypted path: download full blob, decrypt, then serve
        const { Readable } = await import('node:stream')
        const encryptedBlob = await synapseClient.downloadBuffer(obj.pieceCid, copies)
        const plaintext = await encryptionService.decryptBuffer(encryptedBlob)

        if (range) {
          const sliced = plaintext.slice(range.start, range.end + 1)
          reply.raw.writeHead(206, {
            ...baseHeaders,
            'Content-Length': sliced.length,
            'Content-Range': `bytes ${range.start}-${range.end}/${obj.size}`,
          })
          Readable.from(sliced).pipe(reply.raw)
        } else {
          reply.raw.writeHead(200, {
            ...baseHeaders,
            'Content-Length': obj.size,
          })
          Readable.from(plaintext).pipe(reply.raw)
        }
      } else {
        // Unencrypted path: stream directly (existing behavior)
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
      logger.error({ error, bucket, key, pieceCid: obj.pieceCid }, 'download failed')
      sendInternalError(reply, 'Failed to download object from FOC storage')
    }
  })

  // ── POST /{bucket}: DeleteObjects (batch delete) ──────────────────
  app.post('/:bucket', async (request, reply) => {
    const { bucket } = request.params as { bucket: string }
    const query = request.query as Record<string, string>

    if (!metadataStore.bucketExists(bucket)) {
      sendNoSuchBucket(reply, bucket)
      return
    }

    // ── DeleteObjects: POST /{bucket}?delete ─────────────────────────
    if ('delete' in query) {
      // Parse XML body: <Delete><Object><Key>k</Key></Object>...</Delete>
      const body = await readXmlBody(request)
      logger.debug({ bucket, bodyLength: body.length, contentType: request.headers['content-type'] }, 'DeleteObjects')

      const keyMatches = body.match(/<Key>([^<]+)<\/Key>/g) ?? []
      const keys = keyMatches.map((m) => {
        const inner = m.replace(/<Key>/, '').replace(/<\/Key>/, '')
        return decodeURIComponent(inner)
      })

      logger.debug({ bucket, keysToDelete: keys }, 'DeleteObjects parsed keys')

      const deleted: string[] = []
      for (const key of keys) {
        // Clean up local file if staged
        const localPath = metadataStore.getLocalPath(bucket, key)
        if (localPath) {
          localStore.delete(localPath)
        }
        metadataStore.deleteObject(bucket, key)
        deleted.push(key)
      }

      const xml = buildDeleteResultXml(deleted, [])
      reply.header('Content-Type', 'application/xml').send(xml)
      return
    }

    sendS3Error(reply, 400, 'InvalidRequest', 'Unknown POST operation on bucket', bucket)
  })

  // ── POST /{bucket}/{key+}: InitiateMultipartUpload / CompleteMultipartUpload ──
  app.post('/:bucket/*', async (request, reply) => {
    const { bucket } = request.params as { bucket: string; '*': string }
    const key = (request.params as { '*': string })['*']
    const query = request.query as Record<string, string>

    // Trailing slash: POST /bucket/ → treat as bucket-level POST (batch delete)
    if (!key) {
      if (!metadataStore.bucketExists(bucket)) {
        sendNoSuchBucket(reply, bucket)
        return
      }
      if ('delete' in query) {
        const body = await readXmlBody(request)
        logger.debug({ bucket, bodyLength: body.length }, 'DeleteObjects (trailing slash)')
        const keyMatches = body.match(/<Key>([^<]+)<\/Key>/g) ?? []
        const keys = keyMatches.map((m) => {
          const inner = m.replace(/<Key>/, '').replace(/<\/Key>/, '')
          return decodeURIComponent(inner)
        })
        const deleted: string[] = []
        for (const k of keys) {
          const localPath = metadataStore.getLocalPath(bucket, k)
          if (localPath) localStore.delete(localPath)
          metadataStore.deleteObject(bucket, k)
          deleted.push(k)
        }
        const xml = buildDeleteResultXml(deleted, [])
        reply.header('Content-Type', 'application/xml').send(xml)
        return
      }
      sendS3Error(reply, 400, 'InvalidRequest', 'Unknown POST operation on bucket', bucket)
      return
    }

    if (!metadataStore.bucketExists(bucket)) {
      sendNoSuchBucket(reply, bucket)
      return
    }

    // ── InitiateMultipartUpload: POST /{bucket}/{key}?uploads ─────────
    if ('uploads' in query && !('uploadId' in query)) {
      logger.debug({ bucket, key }, 'InitiateMultipartUpload')

      const uploadId = randomUUID()
      const contentType = (request.headers['content-type'] as string | undefined) ?? 'application/octet-stream'
      metadataStore.createMultipartUpload(uploadId, bucket, key, contentType)

      const xml = buildInitiateMultipartUploadXml(bucket, key, uploadId)
      reply.header('Content-Type', 'application/xml').send(xml)
      return
    }

    // ── CompleteMultipartUpload: POST /{bucket}/{key}?uploadId=X ──────
    if ('uploadId' in query) {
      const uploadId = query.uploadId
      logger.debug({ bucket, key, uploadId }, 'CompleteMultipartUpload')

      const upload = metadataStore.getMultipartUpload(uploadId)
      if (!upload || upload.bucket !== bucket || upload.key !== key) {
        sendS3Error(reply, 404, 'NoSuchUpload', 'The specified multipart upload does not exist.', key)
        return
      }

      const parts = metadataStore.getMultipartParts(uploadId)
      if (parts.length === 0) {
        sendS3Error(reply, 400, 'InvalidRequest', 'You must specify at least one part.', key)
        return
      }

      try {
        // Merge parts into a single staged file
        const partNumbers = parts.map((p) => p.partNumber)
        const merged = await localStore.mergeParts(uploadId, partNumbers)

        // Validate merged size
        if (merged.size < MIN_UPLOAD_SIZE) {
          localStore.delete(merged.localPath)
          metadataStore.deleteMultipartUpload(uploadId)
          sendS3Error(
            reply,
            400,
            'EntityTooSmall',
            `Object size ${merged.size} bytes is below minimum ${MIN_UPLOAD_SIZE} bytes required by Filecoin storage providers.`,
            key
          )
          return
        }
        if (merged.size > MAX_UPLOAD_SIZE) {
          localStore.delete(merged.localPath)
          metadataStore.deleteMultipartUpload(uploadId)
          sendS3Error(
            reply,
            400,
            'EntityTooLarge',
            `Object size ${merged.size} bytes exceeds maximum ${MAX_UPLOAD_SIZE} bytes (~1 GiB).`,
            key
          )
          return
        }

        // Stage for async FOC upload
        metadataStore.stageObject(bucket, key, merged.size, upload.contentType, merged.etag, merged.localPath)
        metadataStore.deleteMultipartUpload(uploadId)

        const xml = buildCompleteMultipartUploadXml(bucket, key, merged.etag)
        reply.header('Content-Type', 'application/xml').send(xml)
      } catch (error) {
        logger.error({ error, bucket, key, uploadId }, 'CompleteMultipartUpload failed')
        sendInternalError(reply, 'Failed to complete multipart upload')
      }
      return
    }

    // Unknown POST
    sendS3Error(reply, 400, 'InvalidRequest', 'Unknown POST operation', key)
  })

  // ── PutObject / CopyObject / UploadPart: PUT /{bucket}/{key+} ──────
  app.put('/:bucket/*', async (request, reply) => {
    const { bucket } = request.params as { bucket: string; '*': string }
    const key = (request.params as { '*': string })['*']
    const query = request.query as Record<string, string>

    // Trailing slash: PUT /bucket/ → treat as CreateBucket
    if (!key) {
      logger.debug({ bucket }, 'CreateBucket (trailing slash)')
      if (metadataStore.bucketExists(bucket)) {
        sendS3Error(
          reply,
          409,
          'BucketAlreadyOwnedByYou',
          'Your previous request to create the named bucket succeeded and you already own it.',
          bucket
        )
        return
      }
      metadataStore.createBucket(bucket)
      reply.status(200).header('Location', `/${bucket}`).send()
      return
    }

    if (!metadataStore.bucketExists(bucket)) {
      sendNoSuchBucket(reply, bucket)
      return
    }

    // ── UploadPart: PUT /{bucket}/{key}?partNumber=N&uploadId=X ──────
    if ('partNumber' in query && 'uploadId' in query) {
      const partNumber = Number.parseInt(query.partNumber, 10)
      const uploadId = query.uploadId
      logger.debug({ bucket, key, uploadId, partNumber }, 'UploadPart')

      const upload = metadataStore.getMultipartUpload(uploadId)
      if (!upload || upload.bucket !== bucket || upload.key !== key) {
        sendS3Error(reply, 404, 'NoSuchUpload', 'The specified multipart upload does not exist.', key)
        return
      }

      try {
        const result = await localStore.savePart(uploadId, partNumber, request.raw)
        metadataStore.addMultipartPart(uploadId, partNumber, result.localPath, result.size, result.etag)

        reply.status(200).header('ETag', `"${result.etag}"`).send()
      } catch (error) {
        logger.error({ error, uploadId, partNumber }, 'UploadPart failed')
        sendInternalError(reply, 'Failed to upload part')
      }
      return
    }

    // ── CopyObject: detected by x-amz-copy-source header ───────────
    const copySource = request.headers['x-amz-copy-source'] as string | undefined
    if (copySource) {
      const parsedCopySource = parseCopySource(copySource)
      if (!parsedCopySource) {
        logger.debug({ bucket, key, copySource }, 'CopyObject rejected: invalid source format')
        sendS3Error(reply, 400, 'InvalidArgument', 'Invalid x-amz-copy-source format', copySource)
        return
      }
      const { bucket: srcBucket, key: srcKey } = parsedCopySource

      logger.debug({ bucket, key, copySource, srcBucket, srcKey }, 'CopyObject')

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

    // ── PutObject: async upload (save to disk, return immediately) ────
    logger.debug({ bucket, key }, 'PutObject')

    try {
      const contentLength = request.headers['content-length']
        ? Number.parseInt(request.headers['content-length'] as string, 10)
        : undefined
      const contentType = (request.headers['content-type'] as string | undefined) ?? 'application/octet-stream'

      // Validate size from Content-Length header (fast path, no buffering)
      if (contentLength !== undefined) {
        if (contentLength === 0) {
          // S3 allows empty objects, store metadata only
          const etag = createHash('md5').update('').digest('hex')
          metadataStore.putObject(bucket, key, '', 0, contentType, etag)
          reply.status(200).header('ETag', `"${etag}"`).send()
          return
        }
        if (contentLength < MIN_UPLOAD_SIZE) {
          sendS3Error(
            reply,
            400,
            'EntityTooSmall',
            `Object size ${contentLength} bytes is below minimum ${MIN_UPLOAD_SIZE} bytes required by Filecoin storage providers.`,
            key
          )
          return
        }
        if (contentLength > MAX_UPLOAD_SIZE) {
          sendS3Error(
            reply,
            400,
            'EntityTooLarge',
            `Object size ${contentLength} bytes exceeds maximum ${MAX_UPLOAD_SIZE} bytes (~1 GiB).`,
            key
          )
          return
        }
      }

      // Stage to local disk — this is fast (disk I/O only)
      const stageId = randomUUID()
      const staged = await localStore.stageUpload(stageId, request.raw)

      // Validate after streaming (Content-Length may have been missing)
      if (staged.size < MIN_UPLOAD_SIZE) {
        localStore.delete(staged.localPath)
        sendS3Error(
          reply,
          400,
          'EntityTooSmall',
          `Object size ${staged.size} bytes is below minimum ${MIN_UPLOAD_SIZE} bytes required by Filecoin storage providers.`,
          key
        )
        return
      }
      if (staged.size > MAX_UPLOAD_SIZE) {
        localStore.delete(staged.localPath)
        sendS3Error(
          reply,
          400,
          'EntityTooLarge',
          `Object size ${staged.size} bytes exceeds maximum ${MAX_UPLOAD_SIZE} bytes (~1 GiB).`,
          key
        )
        return
      }

      // Store metadata with status=pending, return immediately
      metadataStore.stageObject(bucket, key, staged.size, contentType, staged.etag, staged.localPath)

      reply.status(200).header('ETag', `"${staged.etag}"`).send()
    } catch (error) {
      logger.error({ error, bucket, key }, 'upload failed')
      sendInternalError(reply, 'Failed to upload object')
    }
  })

  // ── DeleteObject / AbortMultipartUpload: DELETE /{bucket}/{key+} ────
  app.delete('/:bucket/*', async (request, reply) => {
    const { bucket } = request.params as { bucket: string; '*': string }
    const key = (request.params as { '*': string })['*']
    const query = request.query as Record<string, string>

    // Trailing slash: DELETE /bucket/ → treat as DeleteBucket
    if (!key) {
      logger.debug({ bucket }, 'DeleteBucket (trailing slash)')
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
      return
    }

    if (!metadataStore.bucketExists(bucket)) {
      sendNoSuchBucket(reply, bucket)
      return
    }

    // ── AbortMultipartUpload: DELETE /{bucket}/{key}?uploadId=X ──────
    if ('uploadId' in query) {
      const uploadId = query.uploadId
      logger.debug({ bucket, key, uploadId }, 'AbortMultipartUpload')

      const upload = metadataStore.getMultipartUpload(uploadId)
      if (!upload || upload.bucket !== bucket || upload.key !== key) {
        sendS3Error(reply, 404, 'NoSuchUpload', 'The specified multipart upload does not exist.', key)
        return
      }

      // Clean up parts from disk
      localStore.cleanupMultipartDir(uploadId)
      metadataStore.deleteMultipartUpload(uploadId)

      reply.status(204).send()
      return
    }

    // ── DeleteObject ──────────────────────────────────────────────────
    logger.debug({ bucket, key }, 'DeleteObject')

    // Clean up local file if the object is still staged
    const localPath = metadataStore.getLocalPath(bucket, key)
    if (localPath) {
      localStore.delete(localPath)
    }

    // S3 delete is idempotent — always returns 204
    metadataStore.deleteObject(bucket, key)

    reply.status(204).send()
  })
}
