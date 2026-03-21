/**
 * Tests for S3 route handlers using Fastify's built-in injection.
 *
 * SynapseClient is mocked — we're testing the HTTP routing logic,
 * request parsing, and response formatting, not the SDK itself.
 */

import Fastify from 'fastify'
import pino from 'pino'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
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

  beforeEach(async () => {
    app = Fastify({ logger: false })
    metadataStore = new MetadataStore({ dbPath: ':memory:', logger })
    mockSynapse = createMockSynapseClient()

    // Disable body parsing so PutObject can read raw
    app.removeAllContentTypeParsers()
    app.addContentTypeParser('*', (_request, _payload, done) => {
      done(null)
    })

    registerRoutes(app, {
      metadataStore,
      synapseClient: mockSynapse as any,
      logger,
    })
  })

  afterEach(async () => {
    metadataStore.close()
    await app.close()
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
  })

  // ── PutObject: PUT /{bucket}/{key} ──────────────────────────────────

  describe('PUT /:bucket/* (PutObject)', () => {
    it('uploads and stores metadata', async () => {
      const payload = 'x'.repeat(128) // Must be >= 127 bytes (Filecoin SP minimum)
      const response = await app.inject({
        method: 'PUT',
        url: '/default/hello.txt',
        payload,
        headers: { 'content-type': 'text/plain' },
      })

      expect(response.statusCode).toBe(200)
      expect(response.headers['etag']).toBeDefined()

      // Verify synapse upload was called
      expect(mockSynapse.upload).toHaveBeenCalledOnce()

      // Verify metadata was stored
      const obj = metadataStore.getObject('default', 'hello.txt')
      expect(obj).toBeDefined()
      expect(obj?.pieceCid).toBe('baga-test-cid')
    })

    it('returns 404 for invalid bucket', async () => {
      const response = await app.inject({
        method: 'PUT',
        url: '/nonexistent/file.txt',
        payload: 'data',
      })

      expect(response.statusCode).toBe(404)
    })

    it('returns 500 on upload failure', async () => {
      mockSynapse.upload.mockRejectedValueOnce(new Error('Network error'))

      const response = await app.inject({
        method: 'PUT',
        url: '/default/fail.txt',
        payload: 'x'.repeat(128),
      })

      expect(response.statusCode).toBe(500)
      expect(response.body).toContain('InternalError')
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
  })

  // ── GetObject: GET /{bucket}/{key} ──────────────────────────────────

  describe('GET /:bucket/* (GetObject)', () => {
    it('downloads stored object', async () => {
      metadataStore.putObject('default', 'hello.txt', 'baga-cid', 5, 'text/plain', 'etag1')

      const response = await app.inject({ method: 'GET', url: '/default/hello.txt' })

      expect(response.statusCode).toBe(200)
      expect(response.headers['content-type']).toBe('text/plain')
      expect(response.headers['etag']).toBe('"etag1"')
      expect(mockSynapse.download).toHaveBeenCalledWith('baga-cid', [])
      expect(response.body).toBe('Hello')
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
  })
})
