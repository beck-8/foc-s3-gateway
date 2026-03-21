/**
 * Local file store — stages uploads on disk before async FOC upload.
 *
 * Handles two use cases:
 *   1. Regular uploads (PutObject) — saved as a single staged file
 *   2. Multipart uploads — parts saved individually, merged on complete
 *
 * Directory layout:
 *   {dataDir}/staging/{id}           — staged complete files
 *   {dataDir}/multipart/{uploadId}/  — multipart part files
 */

import { createHash } from 'node:crypto'
import {
  createReadStream,
  createWriteStream,
  existsSync,
  mkdirSync,
  readdirSync,
  rmSync,
  statSync,
  unlinkSync,
} from 'node:fs'
import type { IncomingMessage } from 'node:http'
import path from 'node:path'
import type { Logger } from 'pino'

export interface LocalStoreOptions {
  dataDir: string
  logger: Logger
}

export interface StageResult {
  localPath: string
  size: number
  etag: string
}

export class LocalStore {
  readonly stagingDir: string
  readonly multipartDir: string
  private readonly logger: Logger

  constructor(options: LocalStoreOptions) {
    this.logger = options.logger.child({ module: 'local-store' })
    this.stagingDir = path.join(options.dataDir, 'staging')
    this.multipartDir = path.join(options.dataDir, 'multipart')
    mkdirSync(this.stagingDir, { recursive: true })
    mkdirSync(this.multipartDir, { recursive: true })
    this.logger.debug({ stagingDir: this.stagingDir, multipartDir: this.multipartDir }, 'local store initialized')
  }

  /**
   * Stage a request body to a local file.
   * Streams data to disk while computing MD5 for the ETag.
   */
  async stageUpload(id: string, stream: IncomingMessage): Promise<StageResult> {
    const localPath = path.join(this.stagingDir, id)
    return this.writeStream(localPath, stream)
  }

  /**
   * Save one part of a multipart upload to disk.
   */
  async savePart(uploadId: string, partNumber: number, stream: IncomingMessage): Promise<StageResult> {
    const partDir = path.join(this.multipartDir, uploadId)
    mkdirSync(partDir, { recursive: true })
    const localPath = path.join(partDir, `part-${String(partNumber).padStart(5, '0')}`)
    return this.writeStream(localPath, stream)
  }

  /**
   * Merge multipart parts into a single staged file in order.
   * After merging, the parts directory is cleaned up.
   */
  async mergeParts(uploadId: string, partNumbers: number[]): Promise<StageResult> {
    const stagedPath = path.join(this.stagingDir, uploadId)
    const md5 = createHash('md5')
    let totalSize = 0

    const out = createWriteStream(stagedPath)

    for (const num of partNumbers) {
      const partPath = path.join(this.multipartDir, uploadId, `part-${String(num).padStart(5, '0')}`)
      await new Promise<void>((resolve, reject) => {
        const rs = createReadStream(partPath)
        rs.on('data', (chunk) => {
          const buf = Buffer.from(chunk)
          md5.update(buf)
          totalSize += buf.length
        })
        rs.on('error', reject)
        rs.pipe(out, { end: false })
        rs.on('end', resolve)
      })
    }

    await new Promise<void>((resolve, reject) => {
      out.end(() => resolve())
      out.on('error', reject)
    })

    // Clean up parts
    this.cleanupMultipartDir(uploadId)

    return { localPath: stagedPath, size: totalSize, etag: md5.digest('hex') }
  }

  /** Create a file read stream for a staged file */
  createReadStream(localPath: string): import('node:fs').ReadStream {
    return createReadStream(localPath)
  }

  /** Check if a local file exists */
  exists(localPath: string): boolean {
    return existsSync(localPath)
  }

  /** Delete a local file (ignores errors if file is already gone) */
  delete(localPath: string): void {
    try {
      if (existsSync(localPath)) unlinkSync(localPath)
    } catch (error) {
      this.logger.warn({ localPath, error }, 'failed to delete local file')
    }
  }

  /** Remove multipart parts directory for an upload */
  cleanupMultipartDir(uploadId: string): void {
    const partDir = path.join(this.multipartDir, uploadId)
    try {
      if (existsSync(partDir)) {
        rmSync(partDir, { recursive: true, force: true })
      }
    } catch (error) {
      this.logger.warn({ uploadId, error }, 'failed to cleanup multipart parts')
    }
  }

  /** Get disk usage stats for staging and multipart directories */
  getDiskStats(): {
    staging: { count: number; totalBytes: number }
    multipart: { count: number; totalBytes: number }
  } {
    const staging = { count: 0, totalBytes: 0 }
    const multipart = { count: 0, totalBytes: 0 }

    try {
      for (const file of readdirSync(this.stagingDir)) {
        const filePath = path.join(this.stagingDir, file)
        try {
          const stat = statSync(filePath)
          if (stat.isFile()) {
            staging.count++
            staging.totalBytes += stat.size
          }
        } catch {
          // File may have been deleted between readdir and stat
        }
      }
    } catch {
      // Directory may not exist
    }

    try {
      for (const dir of readdirSync(this.multipartDir)) {
        const dirPath = path.join(this.multipartDir, dir)
        try {
          const stat = statSync(dirPath)
          if (stat.isDirectory()) {
            for (const part of readdirSync(dirPath)) {
              const partPath = path.join(dirPath, part)
              try {
                const partStat = statSync(partPath)
                if (partStat.isFile()) {
                  multipart.count++
                  multipart.totalBytes += partStat.size
                }
              } catch {
                // Part file may have been deleted
              }
            }
          }
        } catch {
          // Dir may have been deleted
        }
      }
    } catch {
      // Directory may not exist
    }

    return { staging, multipart }
  }

  /** Recover — on startup, clean up any orphaned staging/multipart files not in the DB */
  cleanupOrphans(knownPaths: Set<string>): void {
    try {
      for (const file of readdirSync(this.stagingDir)) {
        const fullPath = path.join(this.stagingDir, file)
        if (!knownPaths.has(fullPath)) {
          this.logger.info({ path: fullPath }, 'removing orphaned staging file')
          this.delete(fullPath)
        }
      }
      for (const dir of readdirSync(this.multipartDir)) {
        const fullPath = path.join(this.multipartDir, dir)
        // Multipart dirs are always temporary — if we restart, they're orphaned
        this.logger.info({ path: fullPath }, 'removing orphaned multipart directory')
        rmSync(fullPath, { recursive: true, force: true })
      }
    } catch (error) {
      this.logger.warn({ error }, 'cleanup orphans failed')
    }
  }

  /** Internal: stream an IncomingMessage to a file on disk */
  private async writeStream(localPath: string, stream: IncomingMessage): Promise<StageResult> {
    const md5 = createHash('md5')
    let size = 0
    const out = createWriteStream(localPath)

    await new Promise<void>((resolve, reject) => {
      stream.on('data', (chunk: Buffer) => {
        md5.update(chunk)
        size += chunk.length
      })
      stream.pipe(out)
      out.on('finish', resolve)
      stream.on('error', (err) => {
        out.destroy()
        reject(err)
      })
      out.on('error', (err) => {
        stream.destroy()
        reject(err)
      })
    })

    return { localPath, size, etag: md5.digest('hex') }
  }
}
