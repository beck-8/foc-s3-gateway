/**
 * Tests for WebDAV routes.
 */

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import pino from 'pino'
import { MetadataStore } from '../storage/metadata-store.js'
import { createWebDavServer } from './server.js'
import type { FastifyInstance } from 'fastify'

const logger = pino({ level: 'silent' })

describe('WebDAV Routes', () => {
  let app: FastifyInstance
  let metadataStore: MetadataStore

  const mockSynapse = {
    upload: vi.fn().mockResolvedValue({
      pieceCid: 'baga-test',
      size: 100,
      copies: [],
    }),
    download: vi.fn().mockResolvedValue(new Uint8Array([72, 101, 108, 108, 111])),
    getAddress: vi.fn().mockReturnValue('0xtest'),
  }

  beforeEach(async () => {
    metadataStore = new MetadataStore({ dbPath: ':memory:', logger })

    app = await createWebDavServer({
      port: 0,
      host: '127.0.0.1',
      metadataStore,
      synapseClient: mockSynapse as any,
      logger,
    })
  })

  afterEach(async () => {
    metadataStore.close()
    await app.close()
  })

  // ── OPTIONS ──────────────────────────────────────────────────────────

  describe('OPTIONS', () => {
    it('returns DAV header and allowed methods', async () => {
      const response = await app.inject({ method: 'OPTIONS', url: '/test' })

      expect(response.statusCode).toBe(200)
      expect(response.headers['dav']).toBe('1')
      expect(response.headers['allow']).toContain('PROPFIND')
      expect(response.headers['allow']).toContain('PUT')
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
      const response = await app.inject({
        method: 'PUT',
        url: '/default/new-file.txt',
        payload: 'Hello WebDAV',
        headers: { 'content-type': 'text/plain' },
      })

      expect(response.statusCode).toBe(201)
      expect(mockSynapse.upload).toHaveBeenCalled()

      const obj = metadataStore.getObject('default', 'new-file.txt')
      expect(obj).toBeDefined()
      expect(obj?.pieceCid).toBe('baga-test')
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
})
