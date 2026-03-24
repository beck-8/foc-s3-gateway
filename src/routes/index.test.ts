/**
 * Tests for S3 route handlers using Fastify's built-in injection.
 *
 * SynapseClient is mocked — we're testing the HTTP routing logic,
 * request parsing, and response formatting, not the SDK itself.
 */

import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import path from 'node:path'
import Fastify from 'fastify'
import pino from 'pino'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { LocalStore } from '../storage/local-store.js'
import { MetadataStore } from '../storage/metadata-store.js'
import { registerRoutes } from './index.js'

const logger = pino({ level: 'silent' })

/** Minimal mock of SynapseClient — only the methods routes actually call */
function createMockSynapseClient() {
  return {
    getAddress: vi.fn().mockReturnValue('0xMOCK_ADDRESS'),
    upload: vi.fn().mockImplementation(async (data: Uint8Array | ReadableStream<Uint8Array>) => {
      // Consume the stream so MD5 computation completes
      if (data instanceof ReadableStream) {
        const reader = data.getReader()
        while (true) {
          const { done } = await reader.read()
          if (done) break
        }
      }
      return { pieceCid: 'baga-test-cid', size: 1024, copies: [] }
    }),
    download: vi.fn().mockImplementation(async () => {
      const { Readable } = await import('node:stream')
      return {
        stream: Readable.from(Buffer.from('Hello')),
        contentLength: 5,
      }
    }), // "Hello"
  }
}

describe('S3 Routes', () => {
  let app: ReturnType<typeof Fastify>
  let metadataStore: MetadataStore
  let mockSynapse: ReturnType<typeof createMockSynapseClient>
  let localStore: LocalStore
  let tempDir: string

  beforeEach(async () => {
    tempDir = mkdtempSync(path.join(tmpdir(), 's3-routes-test-'))

    app = Fastify({ logger: false })
    metadataStore = new MetadataStore({ dbPath: ':memory:', logger })
    mockSynapse = createMockSynapseClient()
    localStore = new LocalStore({ dataDir: tempDir, logger })

    // Disable body parsing so PutObject can read raw
    app.removeAllContentTypeParsers()
    app.addContentTypeParser('*', (_request: any, _payload: any, done: (err: null) => void) => {
      done(null)
    })

    registerRoutes(app, {
      metadataStore,
      synapseClient: mockSynapse as any,
      localStore,
      logger,
    })
  })

  afterEach(async () => {
    metadataStore.close()
    await app.close()
    rmSync(tempDir, { recursive: true, force: true })
  })

  // ── ListBuckets: GET / ──────────────────────────────────────────────

  describe('GET / (ListBuckets)', () => {
    it('returns XML with default bucket', async () => {
      const response = await app.inject({ method: 'GET', url: '/' })

      expect(response.statusCode).toBe(200)
      expect(response.headers['content-type']).toContain('application/xml')
      expect(response.body).toContain('<Name>default</Name>')
      expect(response.body).toContain('0xMOCK_ADDRESS')
    })
  })

  // ── HeadBucket: HEAD /{bucket} ──────────────────────────────────────

  describe('HEAD /:bucket (HeadBucket)', () => {
    it('returns 200 for valid bucket', async () => {
      const response = await app.inject({ method: 'HEAD', url: '/default' })

      expect(response.statusCode).toBe(200)
    })

    it('returns 404 for invalid bucket', async () => {
      const response = await app.inject({ method: 'HEAD', url: '/nonexistent' })

      expect(response.statusCode).toBe(404)
      expect(response.body).toContain('NoSuchBucket')
    })
  })

  // ── CreateBucket: PUT /{bucket} ─────────────────────────────────────

  describe('PUT /:bucket (CreateBucket)', () => {
    it('creates a new bucket', async () => {
      const response = await app.inject({ method: 'PUT', url: '/photos' })

      expect(response.statusCode).toBe(200)
      expect(response.headers['location']).toBe('/photos')

      // Verify bucket is accessible
      const head = await app.inject({ method: 'HEAD', url: '/photos' })
      expect(head.statusCode).toBe(200)
    })

    it('returns 409 for duplicate bucket', async () => {
      const response = await app.inject({ method: 'PUT', url: '/default' })

      expect(response.statusCode).toBe(409)
      expect(response.body).toContain('BucketAlreadyOwnedByYou')
    })

    it('new bucket appears in ListBuckets', async () => {
      await app.inject({ method: 'PUT', url: '/my-data' })

      const response = await app.inject({ method: 'GET', url: '/' })

      expect(response.body).toContain('<Name>my-data</Name>')
      expect(response.body).toContain('<Name>default</Name>')
    })

    it('can upload to newly created bucket', async () => {
      await app.inject({ method: 'PUT', url: '/photos' })

      const put = await app.inject({
        method: 'PUT',
        url: '/photos/sunset.jpg',
        payload: 'x'.repeat(128),
      })

      expect(put.statusCode).toBe(200)

      const obj = metadataStore.getObject('photos', 'sunset.jpg')
      expect(obj).toBeDefined()
    })
  })

  // ── DeleteBucket: DELETE /{bucket} ──────────────────────────────────

  describe('DELETE /:bucket (DeleteBucket)', () => {
    it('deletes an empty bucket', async () => {
      await app.inject({ method: 'PUT', url: '/temp' })
      const response = await app.inject({ method: 'DELETE', url: '/temp' })

      expect(response.statusCode).toBe(204)

      // Verify bucket is gone
      const head = await app.inject({ method: 'HEAD', url: '/temp' })
      expect(head.statusCode).toBe(404)
    })

    it('returns 409 for non-empty bucket', async () => {
      await app.inject({ method: 'PUT', url: '/data' })
      metadataStore.putObject('data', 'file.txt', 'cid', 10, 'text/plain', 'etag')

      const response = await app.inject({ method: 'DELETE', url: '/data' })

      expect(response.statusCode).toBe(409)
      expect(response.body).toContain('BucketNotEmpty')
    })

    it('returns 404 for non-existent bucket', async () => {
      const response = await app.inject({ method: 'DELETE', url: '/nonexistent' })

      expect(response.statusCode).toBe(404)
    })

    it('returns 409 when deleting default bucket', async () => {
      const response = await app.inject({ method: 'DELETE', url: '/default' })

      expect(response.statusCode).toBe(409)
    })
  })

  // ── ListObjectsV2: GET /{bucket} ───────────────────────────────────

  describe('GET /:bucket (ListObjectsV2)', () => {
    it('returns empty listing for empty bucket', async () => {
      const response = await app.inject({ method: 'GET', url: '/default' })

      expect(response.statusCode).toBe(200)
      expect(response.body).toContain('<KeyCount>0</KeyCount>')
    })

    it('lists stored objects', async () => {
      metadataStore.putObject('default', 'test.txt', 'cid1', 100, 'text/plain', 'e1')
      metadataStore.putObject('default', 'data.bin', 'cid2', 200, 'application/octet-stream', 'e2')

      const response = await app.inject({ method: 'GET', url: '/default' })

      expect(response.statusCode).toBe(200)
      expect(response.body).toContain('<Key>test.txt</Key>')
      expect(response.body).toContain('<Key>data.bin</Key>')
      expect(response.body).toContain('<KeyCount>2</KeyCount>')
    })

    it('filters by prefix', async () => {
      metadataStore.putObject('default', 'dir/a.txt', 'c1', 10, 'text/plain', 'e1')
      metadataStore.putObject('default', 'dir/b.txt', 'c2', 20, 'text/plain', 'e2')
      metadataStore.putObject('default', 'root.txt', 'c3', 30, 'text/plain', 'e3')

      const response = await app.inject({
        method: 'GET',
        url: '/default?prefix=dir/',
      })

      expect(response.body).toContain('<Key>dir/a.txt</Key>')
      expect(response.body).toContain('<Key>dir/b.txt</Key>')
      expect(response.body).not.toContain('root.txt')
    })

    it('returns 404 for invalid bucket', async () => {
      const response = await app.inject({ method: 'GET', url: '/nonexistent' })

      expect(response.statusCode).toBe(404)
    })

    it('GetBucketLocation returns us-east-1', async () => {
      const response = await app.inject({ method: 'GET', url: '/default?location' })

      expect(response.statusCode).toBe(200)
      expect(response.headers['content-type']).toContain('application/xml')
      expect(response.body).toContain('<LocationConstraint')
      expect(response.body).toContain('us-east-1')
    })

    it('GetBucketVersioning returns empty config', async () => {
      const response = await app.inject({ method: 'GET', url: '/default?versioning' })

      expect(response.statusCode).toBe(200)
      expect(response.headers['content-type']).toContain('application/xml')
      expect(response.body).toContain('<VersioningConfiguration')
    })

    it('GetBucketLocation with trailing slash', async () => {
      const response = await app.inject({ method: 'GET', url: '/default/?location' })

      expect(response.statusCode).toBe(200)
      expect(response.body).toContain('us-east-1')
    })

    it('paginates with continuation-token', async () => {
      metadataStore.putObject('default', 'a.txt', 'c1', 10, 'text/plain', 'e1')
      metadataStore.putObject('default', 'b.txt', 'c2', 20, 'text/plain', 'e2')
      metadataStore.putObject('default', 'c.txt', 'c3', 30, 'text/plain', 'e3')

      // First page: max-keys=2 → should be truncated
      const page1 = await app.inject({ method: 'GET', url: '/default?max-keys=2' })
      expect(page1.statusCode).toBe(200)
      expect(page1.body).toContain('<IsTruncated>true</IsTruncated>')
      expect(page1.body).toContain('<NextContinuationToken>')

      const tokenMatch = page1.body.match(/<NextContinuationToken>(.*?)<\/NextContinuationToken>/)
      expect(tokenMatch).toBeTruthy()
      const token = tokenMatch![1]

      // Second page: using continuation-token
      const page2 = await app.inject({ method: 'GET', url: `/default?max-keys=2&continuation-token=${token}` })
      expect(page2.statusCode).toBe(200)
      expect(page2.body).toContain('<IsTruncated>false</IsTruncated>')
      expect(page2.body).toContain('<Key>c.txt</Key>')
    })
  })

  // ── PutObject: PUT /{bucket}/{key} ──────────────────────────────────
  //    Now stages to disk and returns immediately (async upload)

  describe('PUT /:bucket/* (PutObject — async)', () => {
    it('stages upload to disk and returns immediately', async () => {
      const payload = 'x'.repeat(128) // Must be >= 127 bytes (Filecoin SP minimum)
      const response = await app.inject({
        method: 'PUT',
        url: '/default/hello.txt',
        payload,
        headers: { 'content-type': 'text/plain' },
      })

      expect(response.statusCode).toBe(200)
      expect(response.headers['etag']).toBeDefined()

      // Synapse upload should NOT have been called (async, deferred)
      expect(mockSynapse.upload).not.toHaveBeenCalled()

      // Metadata should be stored with status=pending
      const obj = metadataStore.getObject('default', 'hello.txt')
      expect(obj).toBeDefined()
      expect(obj?.size).toBe(128)

      // Local path should be set
      const localPath = metadataStore.getLocalPath('default', 'hello.txt')
      expect(localPath).toBeDefined()
      expect(localStore.exists(localPath!)).toBe(true)
    })

    it('returns 404 for invalid bucket', async () => {
      const response = await app.inject({
        method: 'PUT',
        url: '/nonexistent/file.txt',
        payload: 'data',
      })

      expect(response.statusCode).toBe(404)
    })

    it('rejects files smaller than 127 bytes', async () => {
      const response = await app.inject({
        method: 'PUT',
        url: '/default/tiny.txt',
        payload: 'too small',
        headers: { 'content-type': 'text/plain' },
      })

      expect(response.statusCode).toBe(400)
      expect(response.body).toContain('EntityTooSmall')
    })

    it('accepts empty file (0 bytes)', async () => {
      const response = await app.inject({
        method: 'PUT',
        url: '/default/empty.txt',
        payload: '',
        headers: { 'content-type': 'text/plain', 'content-length': '0' },
      })

      expect(response.statusCode).toBe(200)
      expect(response.headers['etag']).toBeDefined()

      const obj = metadataStore.getObject('default', 'empty.txt')
      expect(obj).toBeDefined()
      expect(obj?.size).toBe(0)
    })

    it('rejects files larger than ~1 GiB (EntityTooLarge)', async () => {
      const response = await app.inject({
        method: 'PUT',
        url: '/default/huge.bin',
        payload: '',
        headers: { 'content-length': '1065353217' }, // 1 GiB + 1 byte
      })

      expect(response.statusCode).toBe(400)
      expect(response.body).toContain('EntityTooLarge')
    })
  })

  // ── CopyObject: PUT /{bucket}/{key} + x-amz-copy-source ────────────

  describe('PUT /:bucket/* with x-amz-copy-source (CopyObject)', () => {
    it('copies object within same bucket (rename)', async () => {
      metadataStore.putObject('default', 'old-name.txt', 'cid1', 100, 'text/plain', 'etag1')

      const response = await app.inject({
        method: 'PUT',
        url: '/default/new-name.txt',
        headers: { 'x-amz-copy-source': '/default/old-name.txt' },
      })

      expect(response.statusCode).toBe(200)
      expect(response.body).toContain('<CopyObjectResult>')
      expect(response.body).toContain('<ETag>')

      // Destination created, source still exists
      expect(metadataStore.getObject('default', 'new-name.txt')?.pieceCid).toBe('cid1')
      expect(metadataStore.getObject('default', 'old-name.txt')).toBeDefined()
    })

    it('copies across buckets', async () => {
      await app.inject({ method: 'PUT', url: '/archive' })
      metadataStore.putObject('default', 'report.pdf', 'cid2', 200, 'application/pdf', 'etag2')

      const response = await app.inject({
        method: 'PUT',
        url: '/archive/report.pdf',
        headers: { 'x-amz-copy-source': 'default/report.pdf' },
      })

      expect(response.statusCode).toBe(200)
      expect(metadataStore.getObject('archive', 'report.pdf')?.pieceCid).toBe('cid2')
    })

    it('returns 404 for non-existent source', async () => {
      const response = await app.inject({
        method: 'PUT',
        url: '/default/dst.txt',
        headers: { 'x-amz-copy-source': '/default/nonexistent.txt' },
      })

      expect(response.statusCode).toBe(404)
      expect(response.body).toContain('NoSuchKey')
    })

    it('does not call synapse upload', async () => {
      metadataStore.putObject('default', 'src.txt', 'cid', 10, 'text/plain', 'etag')

      await app.inject({
        method: 'PUT',
        url: '/default/dst.txt',
        headers: { 'x-amz-copy-source': '/default/src.txt' },
      })

      expect(mockSynapse.upload).not.toHaveBeenCalled()
    })

    it('overwrites existing destination object', async () => {
      metadataStore.putObject('default', 'src.txt', 'cid-src', 100, 'text/plain', 'etag-src')
      metadataStore.putObject('default', 'dst.txt', 'cid-old', 50, 'text/plain', 'etag-old')

      const response = await app.inject({
        method: 'PUT',
        url: '/default/dst.txt',
        headers: { 'x-amz-copy-source': '/default/src.txt' },
      })

      expect(response.statusCode).toBe(200)
      expect(response.body).toContain('<CopyObjectResult>')
      // Destination now points to same pieceCid as source
      expect(metadataStore.getObject('default', 'dst.txt')?.pieceCid).toBe('cid-src')
    })
  })

  // ── GetObject: GET /{bucket}/{key} ──────────────────────────────────

  describe('GET /:bucket/* (GetObject)', () => {
    it('downloads stored object from FOC', async () => {
      metadataStore.putObject('default', 'hello.txt', 'baga-cid', 5, 'text/plain', 'etag1')

      const response = await app.inject({ method: 'GET', url: '/default/hello.txt' })

      expect(response.statusCode).toBe(200)
      expect(response.headers['content-type']).toBe('text/plain')
      expect(response.headers['etag']).toBe('"etag1"')
      expect(mockSynapse.download).toHaveBeenCalledWith('baga-cid', [])
      expect(response.body).toBe('Hello')
    })

    it('serves from local disk when available (local-first)', async () => {
      // Stage a file via PutObject
      const payload = 'x'.repeat(128)
      await app.inject({
        method: 'PUT',
        url: '/default/local.txt',
        payload,
        headers: { 'content-type': 'text/plain' },
      })

      // GetObject should serve from local disk, not FOC
      const response = await app.inject({ method: 'GET', url: '/default/local.txt' })

      expect(response.statusCode).toBe(200)
      expect(response.body).toBe(payload)
      // Should NOT have called synapse download
      expect(mockSynapse.download).not.toHaveBeenCalled()
    })

    it('returns 404 for non-existent object', async () => {
      const response = await app.inject({ method: 'GET', url: '/default/missing.txt' })

      expect(response.statusCode).toBe(404)
      expect(response.body).toContain('NoSuchKey')
    })

    it('returns 500 on download failure', async () => {
      metadataStore.putObject('default', 'broken.txt', 'broken-cid', 10, 'text/plain', 'etag')
      mockSynapse.download.mockRejectedValueOnce(new Error('Provider offline'))

      const response = await app.inject({ method: 'GET', url: '/default/broken.txt' })

      expect(response.statusCode).toBe(500)
    })

    it('returns empty body for 0-byte object', async () => {
      // Put an empty object first
      await app.inject({
        method: 'PUT',
        url: '/default/empty.txt',
        payload: '',
        headers: { 'content-type': 'text/plain', 'content-length': '0' },
      })

      const response = await app.inject({ method: 'GET', url: '/default/empty.txt' })

      expect(response.statusCode).toBe(200)
      expect(response.headers['content-length']).toBe('0')
      expect(response.body).toBe('')
      // Should NOT call synapse download for empty objects
      expect(mockSynapse.download).not.toHaveBeenCalled()
    })
  })

  // ── HeadObject: HEAD /{bucket}/{key} ────────────────────────────────

  describe('HEAD /:bucket/* (HeadObject)', () => {
    it('returns metadata for existing object', async () => {
      metadataStore.putObject('default', 'doc.pdf', 'cid-pdf', 4096, 'application/pdf', 'etag-pdf')

      const response = await app.inject({ method: 'HEAD', url: '/default/doc.pdf' })

      expect(response.statusCode).toBe(200)
      expect(response.headers['content-type']).toBe('application/pdf')
      expect(response.headers['content-length']).toBe('4096')
      expect(response.headers['etag']).toBe('"etag-pdf"')
      expect(response.headers['last-modified']).toBeDefined()
    })

    it('returns 404 for non-existent object', async () => {
      const response = await app.inject({ method: 'HEAD', url: '/default/missing.txt' })
      expect(response.statusCode).toBe(404)
    })

    it('does not call synapse download', async () => {
      metadataStore.putObject('default', 'check.txt', 'cid', 10, 'text/plain', 'etag')

      await app.inject({ method: 'HEAD', url: '/default/check.txt' })

      expect(mockSynapse.download).not.toHaveBeenCalled()
    })
  })

  // ── DeleteObject: DELETE /{bucket}/{key} ────────────────────────────

  describe('DELETE /:bucket/* (DeleteObject)', () => {
    it('deletes existing object and returns 204', async () => {
      metadataStore.putObject('default', 'to-delete.txt', 'cid', 10, 'text/plain', 'etag')

      const response = await app.inject({ method: 'DELETE', url: '/default/to-delete.txt' })

      expect(response.statusCode).toBe(204)
      expect(metadataStore.getObject('default', 'to-delete.txt')).toBeUndefined()
    })

    it('returns 204 for non-existent key (S3 idempotent delete)', async () => {
      const response = await app.inject({ method: 'DELETE', url: '/default/never-existed.txt' })

      expect(response.statusCode).toBe(204)
    })

    it('returns 404 for invalid bucket', async () => {
      const response = await app.inject({ method: 'DELETE', url: '/nonexistent/file.txt' })

      expect(response.statusCode).toBe(404)
    })

    it('cleans up local file on delete of staged object', async () => {
      // Stage a file
      await app.inject({
        method: 'PUT',
        url: '/default/staged.txt',
        payload: 'x'.repeat(128),
      })

      const localPath = metadataStore.getLocalPath('default', 'staged.txt')
      expect(localPath).toBeDefined()
      expect(localStore.exists(localPath!)).toBe(true)

      // Delete it
      await app.inject({ method: 'DELETE', url: '/default/staged.txt' })

      // Local file should be gone
      expect(localStore.exists(localPath!)).toBe(false)
    })
  })

  // ── DeleteObjects: POST /{bucket}?delete ────────────────────────────
  //    Batch delete — used by `mc rm --recursive`

  describe('POST /:bucket?delete (DeleteObjects)', () => {
    it('deletes multiple objects and returns XML result', async () => {
      metadataStore.putObject('default', 'a.txt', 'cid1', 10, 'text/plain', 'e1')
      metadataStore.putObject('default', 'b.txt', 'cid2', 20, 'text/plain', 'e2')

      const body = '<Delete><Object><Key>a.txt</Key></Object><Object><Key>b.txt</Key></Object></Delete>'
      const response = await app.inject({
        method: 'POST',
        url: '/default?delete',
        payload: body,
        headers: { 'content-type': 'application/xml' },
      })

      expect(response.statusCode).toBe(200)
      expect(response.headers['content-type']).toContain('application/xml')
      expect(response.body).toContain('<DeleteResult')
      expect(response.body).toContain('<Deleted><Key>a.txt</Key></Deleted>')
      expect(response.body).toContain('<Deleted><Key>b.txt</Key></Deleted>')

      expect(metadataStore.getObject('default', 'a.txt')).toBeUndefined()
      expect(metadataStore.getObject('default', 'b.txt')).toBeUndefined()
    })

    it('returns empty DeleteResult when keys do not exist (idempotent)', async () => {
      const body = '<Delete><Object><Key>ghost.txt</Key></Object></Delete>'
      const response = await app.inject({
        method: 'POST',
        url: '/default?delete',
        payload: body,
        headers: { 'content-type': 'application/xml' },
      })

      expect(response.statusCode).toBe(200)
      expect(response.body).toContain('<DeleteResult')
      // ghost.txt is "deleted" (idempotent) with no error
      expect(response.body).toContain('<Deleted><Key>ghost.txt</Key></Deleted>')
    })

    it('returns 404 for non-existent bucket', async () => {
      const body = '<Delete><Object><Key>a.txt</Key></Object></Delete>'
      const response = await app.inject({
        method: 'POST',
        url: '/nonexistent?delete',
        payload: body,
        headers: { 'content-type': 'application/xml' },
      })

      expect(response.statusCode).toBe(404)
    })
  })

  // ── Multipart Upload ────────────────────────────────────────────────

  describe('Multipart Upload', () => {
    it('InitiateMultipartUpload returns uploadId', async () => {
      const response = await app.inject({
        method: 'POST',
        url: '/default/big.bin?uploads=',
      })

      expect(response.statusCode).toBe(200)
      expect(response.headers['content-type']).toContain('application/xml')
      expect(response.body).toContain('<InitiateMultipartUploadResult')
      expect(response.body).toContain('<UploadId>')
      expect(response.body).toContain('<Bucket>default</Bucket>')
      expect(response.body).toContain('<Key>big.bin</Key>')
    })

    it('full multipart flow: initiate → upload parts → complete', async () => {
      // 1. Initiate
      const initResp = await app.inject({
        method: 'POST',
        url: '/default/multi.txt?uploads=',
      })
      expect(initResp.statusCode).toBe(200)

      // Extract uploadId from XML
      const uploadIdMatch = initResp.body.match(/<UploadId>(.*?)<\/UploadId>/)
      expect(uploadIdMatch).toBeTruthy()
      const uploadId = uploadIdMatch![1]

      // 2. Upload parts (~128 bytes each, well above minimum when combined)
      const part1Data = 'A'.repeat(128)
      const part1 = await app.inject({
        method: 'PUT',
        url: `/default/multi.txt?partNumber=1&uploadId=${uploadId}`,
        payload: part1Data,
      })
      expect(part1.statusCode).toBe(200)
      expect(part1.headers['etag']).toBeDefined()

      const part2Data = 'B'.repeat(128)
      const part2 = await app.inject({
        method: 'PUT',
        url: `/default/multi.txt?partNumber=2&uploadId=${uploadId}`,
        payload: part2Data,
      })
      expect(part2.statusCode).toBe(200)

      // 3. Complete
      const completeResp = await app.inject({
        method: 'POST',
        url: `/default/multi.txt?uploadId=${uploadId}`,
        payload: '<CompleteMultipartUpload></CompleteMultipartUpload>',
      })
      expect(completeResp.statusCode).toBe(200)
      expect(completeResp.body).toContain('<CompleteMultipartUploadResult')
      expect(completeResp.body).toContain('<ETag>')

      // Object should exist in metadata
      const obj = metadataStore.getObject('default', 'multi.txt')
      expect(obj).toBeDefined()
      expect(obj?.size).toBe(256) // 128 + 128

      // Should be staged for async upload (not directly uploaded)
      expect(mockSynapse.upload).not.toHaveBeenCalled()
      const localPath = metadataStore.getLocalPath('default', 'multi.txt')
      expect(localPath).toBeDefined()
    })

    it('AbortMultipartUpload cleans up', async () => {
      // Initiate
      const initResp = await app.inject({
        method: 'POST',
        url: '/default/aborted.txt?uploads=',
      })
      const uploadId = initResp.body.match(/<UploadId>(.*?)<\/UploadId>/)![1]

      // Upload a part
      await app.inject({
        method: 'PUT',
        url: `/default/aborted.txt?partNumber=1&uploadId=${uploadId}`,
        payload: 'x'.repeat(128),
      })

      // Abort
      const abortResp = await app.inject({
        method: 'DELETE',
        url: `/default/aborted.txt?uploadId=${uploadId}`,
      })
      expect(abortResp.statusCode).toBe(204)

      // Upload session should be gone
      expect(metadataStore.getMultipartUpload(uploadId)).toBeUndefined()
    })

    it('returns 404 for non-existent uploadId on UploadPart', async () => {
      const response = await app.inject({
        method: 'PUT',
        url: '/default/file.txt?partNumber=1&uploadId=fake-id',
        payload: 'data',
      })
      expect(response.statusCode).toBe(404)
      expect(response.body).toContain('NoSuchUpload')
    })

    it('returns 404 for non-existent uploadId on Complete', async () => {
      const response = await app.inject({
        method: 'POST',
        url: '/default/file.txt?uploadId=fake-id',
      })
      expect(response.statusCode).toBe(404)
      expect(response.body).toContain('NoSuchUpload')
    })

    it('returns 404 for non-existent bucket on Initiate', async () => {
      const response = await app.inject({
        method: 'POST',
        url: '/nonexistent/file.txt?uploads=',
      })
      expect(response.statusCode).toBe(404)
      expect(response.body).toContain('NoSuchBucket')
    })
  })
})
