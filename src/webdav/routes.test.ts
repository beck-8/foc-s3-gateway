/**
 * Tests for WebDAV routes.
 */

import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import path from 'node:path'
import type { FastifyInstance } from 'fastify'
import pino from 'pino'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { LocalStore } from '../storage/local-store.js'
import { MetadataStore } from '../storage/metadata-store.js'
import { createWebDavServer } from './server.js'

const logger = pino({ level: 'silent' })

describe('WebDAV Routes', () => {
  let app: FastifyInstance
  let metadataStore: MetadataStore
  let localStore: LocalStore
  let tempDir: string

  const mockSynapse = {
    upload: vi.fn().mockImplementation(async (data: Uint8Array | ReadableStream<Uint8Array>) => {
      if (data instanceof ReadableStream) {
        const reader = data.getReader()
        while (true) {
          const { done } = await reader.read()
          if (done) break
        }
      }
      return { pieceCid: 'baga-test', size: 100, copies: [] }
    }),
    download: vi.fn().mockImplementation(async () => {
      const { Readable } = await import('node:stream')
      return {
        stream: Readable.from(Buffer.from('Hello')),
        contentLength: 5,
      }
    }),
    getAddress: vi.fn().mockReturnValue('0xtest'),
  }

  beforeEach(async () => {
    tempDir = mkdtempSync(path.join(tmpdir(), 'webdav-test-'))
    metadataStore = new MetadataStore({ dbPath: ':memory:', logger })
    localStore = new LocalStore({ dataDir: tempDir, logger })

    // Reset mock call counts so each test starts clean
    mockSynapse.upload.mockClear()
    mockSynapse.download.mockClear()

    app = await createWebDavServer({
      port: 0,
      host: '127.0.0.1',
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

  // ── OPTIONS ──────────────────────────────────────────────────────────

  describe('OPTIONS', () => {
    it('returns DAV header and allowed methods', async () => {
      const response = await app.inject({ method: 'OPTIONS', url: '/test' })

      expect(response.statusCode).toBe(200)
      expect(response.headers.dav).toBe('1')
      expect(response.headers.allow).toContain('PROPFIND')
      expect(response.headers.allow).toContain('PUT')
    })
  })

  // ── PROPFIND ─────────────────────────────────────────────────────────

  describe('PROPFIND', () => {
    it('lists buckets at root', async () => {
      const response = await app.inject({
        method: 'PROPFIND',
        url: '/',
        headers: { depth: '1' },
      })

      expect(response.statusCode).toBe(207)
      expect(response.body).toContain('<D:multistatus')
      expect(response.body).toContain('default')
    })

    it('lists files in bucket', async () => {
      metadataStore.putObject('default', 'hello.txt', 'cid1', 5, 'text/plain', 'etag1')
      metadataStore.putObject('default', 'world.txt', 'cid2', 10, 'text/plain', 'etag2')

      const response = await app.inject({
        method: 'PROPFIND',
        url: '/default/',
        headers: { depth: '1' },
      })

      expect(response.statusCode).toBe(207)
      expect(response.body).toContain('hello.txt')
      expect(response.body).toContain('world.txt')
      expect(response.body).toContain('<D:getcontentlength>')
    })

    it('returns 404 for non-existent bucket', async () => {
      const response = await app.inject({
        method: 'PROPFIND',
        url: '/nonexistent/',
        headers: { depth: '0' },
      })

      expect(response.statusCode).toBe(404)
    })

    it('returns file properties at depth 0', async () => {
      metadataStore.putObject('default', 'doc.pdf', 'cid3', 1024, 'application/pdf', 'etag3')

      const response = await app.inject({
        method: 'PROPFIND',
        url: '/default/doc.pdf',
        headers: { depth: '0' },
      })

      expect(response.statusCode).toBe(207)
      expect(response.body).toContain('doc.pdf')
      expect(response.body).toContain('1024')
      expect(response.body).toContain('application/pdf')
    })
  })

  // ── GET / PUT ────────────────────────────────────────────────────────

  describe('GET (download)', () => {
    it('downloads a file', async () => {
      metadataStore.putObject('default', 'test.txt', 'cid1', 5, 'text/plain', 'etag1')

      const response = await app.inject({ method: 'GET', url: '/default/test.txt' })

      expect(response.statusCode).toBe(200)
      expect(response.headers['content-type']).toBe('text/plain')
      expect(response.body).toBe('Hello')
    })

    it('returns 404 for missing file', async () => {
      const response = await app.inject({ method: 'GET', url: '/default/nope.txt' })
      expect(response.statusCode).toBe(404)
    })
  })

  describe('PUT (upload)', () => {
    it('uploads a file', async () => {
      // Payload must be >= 127 bytes (Filecoin SP minimum)
      const payload = 'x'.repeat(128)
      const response = await app.inject({
        method: 'PUT',
        url: '/default/new-file.txt',
        payload,
        headers: { 'content-type': 'text/plain' },
      })

      expect(response.statusCode).toBe(201)

      // Async: synapse upload should NOT have been called (deferred to worker)
      expect(mockSynapse.upload).not.toHaveBeenCalled()

      const obj = metadataStore.getObject('default', 'new-file.txt')
      expect(obj).toBeDefined()
      expect(obj?.size).toBe(128)

      // Should be staged locally
      const localPath = metadataStore.getLocalPath('default', 'new-file.txt')
      expect(localPath).toBeDefined()
    })

    it('rejects files smaller than 127 bytes', async () => {
      const response = await app.inject({
        method: 'PUT',
        url: '/default/tiny.txt',
        payload: 'too small',
        headers: { 'content-type': 'text/plain' },
      })

      expect(response.statusCode).toBe(400)
      expect(response.body).toContain('too small')
    })
  })

  // ── DELETE ───────────────────────────────────────────────────────────

  describe('DELETE', () => {
    it('deletes a file', async () => {
      metadataStore.putObject('default', 'rm-me.txt', 'cid', 10, 'text/plain', 'etag')

      const response = await app.inject({ method: 'DELETE', url: '/default/rm-me.txt' })

      expect(response.statusCode).toBe(204)
      expect(metadataStore.getObject('default', 'rm-me.txt')).toBeUndefined()
    })

    it('refuses to delete a bucket with active multipart uploads', async () => {
      metadataStore.createBucket('uploads')
      metadataStore.createMultipartUpload('upload-1', 'uploads', 'movie.bin', 'application/octet-stream')

      const response = await app.inject({ method: 'DELETE', url: '/uploads' })

      expect(response.statusCode).toBe(409)
      expect(metadataStore.bucketExists('uploads')).toBe(true)
    })
  })

  // ── MKCOL ────────────────────────────────────────────────────────────

  describe('MKCOL', () => {
    it('creates a bucket', async () => {
      const response = await app.inject({ method: 'MKCOL', url: '/my-bucket' })

      expect(response.statusCode).toBe(201)
      expect(metadataStore.bucketExists('my-bucket')).toBe(true)
    })

    it('returns 405 for existing bucket', async () => {
      const response = await app.inject({ method: 'MKCOL', url: '/default' })

      expect(response.statusCode).toBe(405)
    })
  })

  // ── COPY / MOVE ─────────────────────────────────────────────────────

  describe('COPY', () => {
    it('copies a file', async () => {
      metadataStore.putObject('default', 'original.txt', 'cid1', 100, 'text/plain', 'etag1')

      const response = await app.inject({
        method: 'COPY',
        url: '/default/original.txt',
        headers: { destination: 'http://localhost/default/copy.txt' },
      })

      expect(response.statusCode).toBe(201)
      expect(metadataStore.getObject('default', 'copy.txt')?.pieceCid).toBe('cid1')
      expect(metadataStore.getObject('default', 'original.txt')).toBeDefined()
    })

    it('rejects destination buckets that do not exist', async () => {
      metadataStore.putObject('default', 'original.txt', 'cid1', 100, 'text/plain', 'etag1')

      const response = await app.inject({
        method: 'COPY',
        url: '/default/original.txt',
        headers: { destination: 'http://localhost/missing/copy.txt' },
      })

      expect(response.statusCode).toBe(409)
      expect(metadataStore.getObject('missing', 'copy.txt')).toBeUndefined()
      expect(metadataStore.getObject('default', 'original.txt')).toBeDefined()
    })
  })

  describe('MOVE', () => {
    it('moves/renames a file', async () => {
      metadataStore.putObject('default', 'old-name.txt', 'cid1', 100, 'text/plain', 'etag1')

      const response = await app.inject({
        method: 'MOVE',
        url: '/default/old-name.txt',
        headers: { destination: 'http://localhost/default/new-name.txt' },
      })

      expect(response.statusCode).toBe(201)
      expect(metadataStore.getObject('default', 'new-name.txt')?.pieceCid).toBe('cid1')
      expect(metadataStore.getObject('default', 'old-name.txt')).toBeUndefined()
    })

    it('rejects destination buckets that do not exist without deleting source', async () => {
      metadataStore.putObject('default', 'old-name.txt', 'cid1', 100, 'text/plain', 'etag1')

      const response = await app.inject({
        method: 'MOVE',
        url: '/default/old-name.txt',
        headers: { destination: 'http://localhost/missing/new-name.txt' },
      })

      expect(response.statusCode).toBe(409)
      expect(metadataStore.getObject('missing', 'new-name.txt')).toBeUndefined()
      expect(metadataStore.getObject('default', 'old-name.txt')).toBeDefined()
    })
  })

  // ── LOCK / UNLOCK (stubs) ───────────────────────────────────────────

  describe('LOCK/UNLOCK (stubs)', () => {
    it('LOCK returns lock token', async () => {
      const response = await app.inject({ method: 'LOCK', url: '/default/file.txt' })

      expect(response.statusCode).toBe(200)
      expect(response.headers['lock-token']).toBeDefined()
    })

    it('UNLOCK returns 204', async () => {
      const response = await app.inject({ method: 'UNLOCK', url: '/default/file.txt' })

      expect(response.statusCode).toBe(204)
    })
  })

  // ── HEAD ──────────────────────────────────────────────────────────────

  describe('HEAD (file metadata)', () => {
    it('returns headers for existing file', async () => {
      metadataStore.putObject('default', 'info.txt', 'cid1', 512, 'text/plain', 'etag-info')

      const response = await app.inject({ method: 'HEAD', url: '/default/info.txt' })

      expect(response.statusCode).toBe(200)
      expect(response.headers['content-type']).toBe('text/plain')
      expect(response.headers['content-length']).toBe('512')
      expect(response.headers.etag).toBe('"etag-info"')
      // Note: Fastify auto-generates HEAD from GET via raw stream; body may not be empty
      // in inject mode. The protocol-level guarantee is that real HTTP HEAD strips body.
    })

    it('returns 404 for non-existent file', async () => {
      const response = await app.inject({ method: 'HEAD', url: '/default/phantom.txt' })
      expect(response.statusCode).toBe(404)
    })
  })

  // ── PROPFIND: additional scenarios ───────────────────────────────────

  describe('PROPFIND (additional)', () => {
    it('Depth:0 at bucket level returns only the bucket itself', async () => {
      metadataStore.putObject('default', 'hidden.txt', 'cid', 10, 'text/plain', 'e')

      const response = await app.inject({
        method: 'PROPFIND',
        url: '/default/',
        headers: { depth: '0' },
      })

      expect(response.statusCode).toBe(207)
      // Should mention the bucket
      expect(response.body).toContain('default')
      // Should NOT list child files at Depth:0
      expect(response.body).not.toContain('hidden.txt')
    })

    it('lists virtual subdirectory when keys contain /', async () => {
      metadataStore.putObject('default', 'photos/2025/img.jpg', 'c1', 100, 'image/jpeg', 'e1')
      metadataStore.putObject('default', 'photos/2025/raw.cr2', 'c2', 200, 'image/x-canon-cr2', 'e2')

      const response = await app.inject({
        method: 'PROPFIND',
        url: '/default/photos/',
        headers: { depth: '1' },
      })

      expect(response.statusCode).toBe(207)
      // Should show the virtual "2025/" subdirectory as a collection
      expect(response.body).toContain('2025')
    })
  })

  // ── MKCOL: key-level (stub) ──────────────────────────────────────────

  describe('MKCOL (additional)', () => {
    it('returns 201 for key-level MKCOL (stub — virtual subdirs)', async () => {
      // Key-level MKCOL is a stub: we don't actually create directories in the DB,
      // but return 201 to satisfy WebDAV clients that create intermediate folders.
      const response = await app.inject({ method: 'MKCOL', url: '/default/subfolder' })
      expect(response.statusCode).toBe(201)
    })
  })

  // ── COPY: error cases ────────────────────────────────────────────────

  describe('COPY (error cases)', () => {
    it('returns 404 when source file does not exist', async () => {
      const response = await app.inject({
        method: 'COPY',
        url: '/default/nonexistent.txt',
        headers: { destination: 'http://localhost/default/copy.txt' },
      })

      expect(response.statusCode).toBe(404)
    })
  })

  // ── MOVE: error cases ────────────────────────────────────────────────

  describe('MOVE (error cases)', () => {
    it('returns 404 when source file does not exist', async () => {
      const response = await app.inject({
        method: 'MOVE',
        url: '/default/ghost.txt',
        headers: { destination: 'http://localhost/default/moved.txt' },
      })

      expect(response.statusCode).toBe(404)
    })
  })

  // ── DELETE: additional scenarios ─────────────────────────────────────

  describe('DELETE (additional)', () => {
    it('returns 409 when deleting non-empty bucket', async () => {
      await app.inject({ method: 'MKCOL', url: '/photos' })
      metadataStore.putObject('photos', 'img.jpg', 'cid', 100, 'image/jpeg', 'e')

      const response = await app.inject({ method: 'DELETE', url: '/photos' })

      expect(response.statusCode).toBe(409)
    })

    it('cleans up staging file when deleting a staged object', async () => {
      const payload = 'x'.repeat(128)
      await app.inject({
        method: 'PUT',
        url: '/default/staged.bin',
        payload,
        headers: { 'content-type': 'application/octet-stream' },
      })

      const localPath = metadataStore.getLocalPath('default', 'staged.bin')
      expect(localPath).toBeDefined()
      if (localPath === undefined) {
        throw new Error('expected local path for staged object')
      }
      expect(localStore.exists(localPath)).toBe(true)

      await app.inject({ method: 'DELETE', url: '/default/staged.bin' })

      expect(localStore.exists(localPath)).toBe(false)
    })
  })

  // ── PROPPATCH (stub) ─────────────────────────────────────────────────

  describe('PROPPATCH (stub)', () => {
    it('returns 207 multistatus', async () => {
      const response = await app.inject({
        method: 'PROPPATCH',
        url: '/default/file.txt',
        payload: '<D:propertyupdate xmlns:D="DAV:"><D:set><D:prop/></D:set></D:propertyupdate>',
        headers: { 'content-type': 'application/xml' },
      })

      expect(response.statusCode).toBe(207)
      expect(response.headers['content-type']).toContain('application/xml')
      expect(response.body).toContain('<D:multistatus')
    })
  })

  // ── PUT: root / bucket-level rejection ──────────────────────────────

  describe('PUT (root / bucket-level rejection)', () => {
    it('returns 409 when writing to root', async () => {
      const response = await app.inject({
        method: 'PUT',
        url: '/',
        payload: 'data',
      })

      expect(response.statusCode).toBe(409)
    })

    it('returns 409 when writing to bucket level (no key)', async () => {
      const response = await app.inject({
        method: 'PUT',
        url: '/default/',
        payload: 'data',
      })

      expect(response.statusCode).toBe(409)
    })
  })

  // ── GET: 0-byte file ─────────────────────────────────────────────────

  describe('GET (0-byte file)', () => {
    it('returns 200 with empty body for a 0-byte object', async () => {
      // Create a 0-byte object directly in the metadata store
      metadataStore.putObject(
        'default',
        'empty.dat',
        '',
        0,
        'application/octet-stream',
        'd41d8cd98f00b204e9800998ecf8427e'
      )

      // Reset mock so previous test calls don't bleed through
      mockSynapse.download.mockClear()

      const response = await app.inject({ method: 'GET', url: '/default/empty.dat' })

      expect(response.statusCode).toBe(200)
      expect(Number(response.headers['content-length'])).toBe(0)
      expect(response.body).toBe('')
      // Should NOT call synapse download for empty objects
      expect(mockSynapse.download).not.toHaveBeenCalled()
    })
  })

  // ── GET: local-first ─────────────────────────────────────────────────

  describe('GET (local-first)', () => {
    it('serves staged file from local disk, not FOC', async () => {
      const payload = 'x'.repeat(128)
      // Stage via PUT
      await app.inject({
        method: 'PUT',
        url: '/default/local.bin',
        payload,
        headers: { 'content-type': 'application/octet-stream' },
      })

      // Reset the mock to detect if download is called
      mockSynapse.download.mockClear()

      const response = await app.inject({ method: 'GET', url: '/default/local.bin' })

      expect(response.statusCode).toBe(200)
      expect(response.body).toBe(payload)
      // Synapse download must NOT have been called
      expect(mockSynapse.download).not.toHaveBeenCalled()
    })
  })

  // ── GET: byte range ─────────────────────────────────────────────────

  describe('GET (byte range)', () => {
    it('returns 206 with partial content for Range request from local disk', async () => {
      const payload = 'x'.repeat(128)
      await app.inject({
        method: 'PUT',
        url: '/default/range.bin',
        payload,
        headers: { 'content-type': 'application/octet-stream' },
      })

      const response = await app.inject({
        method: 'GET',
        url: '/default/range.bin',
        headers: { range: 'bytes=0-9' },
      })

      expect(response.statusCode).toBe(206)
      expect(response.headers['content-range']).toBe('bytes 0-9/128')
      expect(response.headers['content-length']).toBe('10')
      expect(response.headers['accept-ranges']).toBe('bytes')
      expect(response.body).toBe('x'.repeat(10))
    })

    it('returns 416 for out-of-range Range request', async () => {
      const payload = 'x'.repeat(128)
      await app.inject({
        method: 'PUT',
        url: '/default/small.bin',
        payload,
        headers: { 'content-type': 'application/octet-stream' },
      })

      const response = await app.inject({
        method: 'GET',
        url: '/default/small.bin',
        headers: { range: 'bytes=200-300' },
      })

      expect(response.statusCode).toBe(416)
      expect(response.headers['content-range']).toBe('bytes */128')
    })

    it('returns Accept-Ranges header on full download', async () => {
      metadataStore.putObject('default', 'info.txt', 'cid1', 512, 'text/plain', 'etag-info')

      const response = await app.inject({ method: 'GET', url: '/default/info.txt' })

      expect(response.statusCode).toBe(200)
      expect(response.headers['accept-ranges']).toBe('bytes')
    })
  })
})
