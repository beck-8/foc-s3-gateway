/**
 * Tests for authentication middleware.
 */

import Fastify from 'fastify'
import pino from 'pino'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { createAuthHook } from './index.js'

const logger = pino({ level: 'silent' })
const TEST_AK = 'my-access-key'
const TEST_SK = 'my-secret-key'

describe('Auth Middleware', () => {
  let app: ReturnType<typeof Fastify>

  beforeEach(async () => {
    app = Fastify({ logger: false })

    const authHook = createAuthHook({ accessKey: TEST_AK, secretKey: TEST_SK, logger })
    app.addHook('preHandler', authHook)

    // A simple test route
    app.get('/test', async () => ({ ok: true }))
    app.get('/webdav/file.txt', async () => 'file content')
  })

  afterEach(async () => {
    await app.close()
  })

  // ── S3 Auth (AWS Sig V4) ────────────────────────────────────────────

  describe('S3 Auth (AWS Sig V4)', () => {
    it('allows valid access key in Sig V4 header', async () => {
      const response = await app.inject({
        method: 'GET',
        url: '/test',
        headers: {
          authorization: `AWS4-HMAC-SHA256 Credential=${TEST_AK}/20250321/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abc123`,
        },
      })

      expect(response.statusCode).toBe(200)
    })

    it('rejects invalid access key', async () => {
      const response = await app.inject({
        method: 'GET',
        url: '/test',
        headers: {
          authorization:
            'AWS4-HMAC-SHA256 Credential=wrong-key/20250321/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=abc',
        },
      })

      expect(response.statusCode).toBe(403)
      expect(response.body).toContain('AccessDenied')
    })

    it('rejects missing auth header', async () => {
      const response = await app.inject({ method: 'GET', url: '/test' })

      expect(response.statusCode).toBe(403)
    })
  })

  // ── WebDAV Auth (Basic) ─────────────────────────────────────────────

  describe('WebDAV Auth (Basic)', () => {
    it('allows valid basic auth credentials', async () => {
      const encoded = Buffer.from(`${TEST_AK}:${TEST_SK}`).toString('base64')

      const response = await app.inject({
        method: 'GET',
        url: '/webdav/file.txt',
        headers: { authorization: `Basic ${encoded}` },
      })

      expect(response.statusCode).toBe(200)
    })

    it('rejects wrong password', async () => {
      const encoded = Buffer.from(`${TEST_AK}:wrong-password`).toString('base64')

      const response = await app.inject({
        method: 'GET',
        url: '/webdav/file.txt',
        headers: { authorization: `Basic ${encoded}` },
      })

      expect(response.statusCode).toBe(401)
      expect(response.headers['www-authenticate']).toContain('Basic')
    })

    it('rejects wrong username', async () => {
      const encoded = Buffer.from(`wrong-user:${TEST_SK}`).toString('base64')

      const response = await app.inject({
        method: 'GET',
        url: '/webdav/file.txt',
        headers: { authorization: `Basic ${encoded}` },
      })

      expect(response.statusCode).toBe(401)
    })
  })

  // ── Edge cases ──────────────────────────────────────────────────────

  describe('Edge cases', () => {
    it('rejects unknown auth scheme', async () => {
      const response = await app.inject({
        method: 'GET',
        url: '/test',
        headers: { authorization: 'Bearer some-token' },
      })

      expect(response.statusCode).toBe(403)
    })
  })
})
