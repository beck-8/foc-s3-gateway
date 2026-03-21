/** S3 error helpers */

import type { FastifyReply } from 'fastify'
import { buildErrorXml } from './xml.js'

function generateRequestId(): string {
  return Math.random().toString(36).substring(2, 18).toUpperCase()
}

export function sendS3Error(
  reply: FastifyReply,
  statusCode: number,
  code: string,
  message: string,
  resource?: string
): void {
  const xml = buildErrorXml({
    code,
    message,
    resource,
    requestId: generateRequestId(),
  })

  reply.status(statusCode).header('Content-Type', 'application/xml').send(xml)
}

export function sendNoSuchKey(reply: FastifyReply, key: string): void {
  sendS3Error(reply, 404, 'NoSuchKey', 'The specified key does not exist.', key)
}

export function sendNoSuchBucket(reply: FastifyReply, bucket: string): void {
  sendS3Error(reply, 404, 'NoSuchBucket', 'The specified bucket does not exist.', bucket)
}

export function sendInternalError(reply: FastifyReply, message: string): void {
  sendS3Error(reply, 500, 'InternalError', message)
}
