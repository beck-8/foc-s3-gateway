/**
 * Authentication middleware for S3 and WebDAV.
 *
 * S3: Extracts Access Key from AWS Signature V4 Authorization header.
 * WebDAV: Extracts credentials from HTTP Basic Auth header.
 * Only validates Access Key match — does not verify signature (simplified auth).
 */

import type { FastifyReply, FastifyRequest } from 'fastify'
import type { Logger } from 'pino'
import { buildErrorXml } from '../s3/xml.js'

export interface AuthOptions {
  accessKey: string
  secretKey: string
  logger: Logger
}

/**
 * Extract the Access Key ID from an AWS Signature V4 Authorization header.
 *
 * Format: AWS4-HMAC-SHA256 Credential=AKID/20250321/us-east-1/s3/aws4_request, ...
 */
function extractS3AccessKey(authHeader: string): string | undefined {
  // AWS4-HMAC-SHA256 Credential=<ak>/date/region/service/aws4_request
  const match = authHeader.match(/Credential=([^/]+)\//)
  return match?.[1]
}

/**
 * Decode HTTP Basic Auth header.
 * Format: Basic base64(username:password)
 */
function extractBasicAuth(authHeader: string): { username: string; password: string } | undefined {
  if (!authHeader.startsWith('Basic ')) return undefined

  try {
    const decoded = Buffer.from(authHeader.slice(6), 'base64').toString('utf8')
    const colonIdx = decoded.indexOf(':')
    if (colonIdx < 0) return undefined
    return {
      username: decoded.slice(0, colonIdx),
      password: decoded.slice(colonIdx + 1),
    }
  } catch {
    return undefined
  }
}

export function createAuthHook(options: AuthOptions) {
  const { accessKey, secretKey, logger } = options

  return async function authHook(request: FastifyRequest, reply: FastifyReply): Promise<void> {
    const authHeader = request.headers.authorization

    // No auth header → check for presigned URL (query string auth)
    if (!authHeader) {
      const query = request.query as Record<string, string>

      // S3 Presigned URL: ?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AK/date/region/s3/aws4_request
      if (query['X-Amz-Algorithm'] === 'AWS4-HMAC-SHA256' && query['X-Amz-Credential']) {
        const ak = query['X-Amz-Credential'].split('/')[0]
        if (ak !== accessKey) {
          logger.warn({ url: request.url, providedAK: ak }, 'invalid presigned URL access key')
          sendAuthError(reply, request.url)
          return
        }
        return // Presigned URL access key matches → authorized
      }

      // Skip auth for internal endpoints (status API)
      if (request.url.startsWith('/_/')) {
        return
      }

      logger.warn({ url: request.url, method: request.method }, 'missing auth header')
      sendAuthError(reply, request.url)
      return
    }

    // S3: AWS Signature V4
    if (authHeader.startsWith('AWS4-HMAC-SHA256')) {
      const ak = extractS3AccessKey(authHeader)
      if (ak !== accessKey) {
        logger.warn({ url: request.url, providedAK: ak }, 'invalid S3 access key')
        sendAuthError(reply, request.url)
        return
      }
      return // Access key matches → authorized
    }

    // WebDAV / Basic Auth
    if (authHeader.startsWith('Basic ')) {
      const creds = extractBasicAuth(authHeader)
      if (!creds || creds.username !== accessKey || creds.password !== secretKey) {
        logger.warn({ url: request.url }, 'invalid basic auth credentials')
        reply.status(401).header('WWW-Authenticate', 'Basic realm="FOC Gateway"').send('Unauthorized')
        return
      }
      return // Credentials match → authorized
    }

    // Unknown auth scheme
    logger.warn({ url: request.url, scheme: authHeader.split(' ')[0] }, 'unsupported auth scheme')
    sendAuthError(reply, request.url)
  }
}

function sendAuthError(reply: FastifyReply, resource: string): void {
  const xml = buildErrorXml({
    code: 'AccessDenied',
    message: 'Access Denied',
    resource,
    requestId: Math.random().toString(36).substring(2, 18).toUpperCase(),
  })
  reply.status(403).header('Content-Type', 'application/xml').send(xml)
}
