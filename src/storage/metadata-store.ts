/**
 * SQLite metadata store for mapping S3 object keys to PieceCIDs.
 *
 * This is the local index that maps S3-style paths to FOC storage locations.
 * Without this, we'd have no way to know which PieceCID corresponds to which key.
 */

import Database from 'better-sqlite3'
import type { Logger } from 'pino'
import type { S3Bucket, S3Object } from '../s3/types.js'
import type { CopyInfo } from './synapse-client.js'

export interface MetadataStoreOptions {
  dbPath: string
  logger: Logger
}

export interface PutObjectOptions {
  bucket: string
  key: string
  pieceCid: string
  size: number
  contentType: string
  etag: string
  copies?: CopyInfo[] | undefined
}

export class MetadataStore {
  private readonly db: Database.Database
  private readonly logger: Logger

  constructor(options: MetadataStoreOptions) {
    this.logger = options.logger.child({ module: 'metadata-store' })
    this.db = new Database(options.dbPath)
    this.db.pragma('journal_mode = WAL')
    this.db.pragma('synchronous = NORMAL')
    this.db.pragma('cache_size = -20000')
    this.db.pragma('busy_timeout = 5000')
    this.db.pragma('temp_store = MEMORY')
    this.db.pragma('mmap_size = 268435456')
    this.initSchema()
  }

  private initSchema(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS buckets (
        name TEXT PRIMARY KEY,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
      );

      CREATE TABLE IF NOT EXISTS objects (
        bucket TEXT NOT NULL,
        key TEXT NOT NULL,
        piece_cid TEXT NOT NULL,
        size INTEGER NOT NULL,
        content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
        etag TEXT NOT NULL,
        copies_count INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now')),
        deleted INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (bucket, key)
      );

      CREATE TABLE IF NOT EXISTS object_copies (
        bucket TEXT NOT NULL,
        key TEXT NOT NULL,
        provider_id TEXT NOT NULL,
        data_set_id TEXT NOT NULL,
        retrieval_url TEXT NOT NULL,
        role TEXT NOT NULL DEFAULT 'primary',
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        PRIMARY KEY (bucket, key, provider_id)
      );

      CREATE INDEX IF NOT EXISTS idx_objects_bucket ON objects(bucket);
      CREATE INDEX IF NOT EXISTS idx_objects_prefix ON objects(bucket, key);
      CREATE INDEX IF NOT EXISTS idx_objects_piece_cid ON objects(piece_cid);
      CREATE INDEX IF NOT EXISTS idx_copies_piece ON object_copies(bucket, key);
    `)

    // Ensure default bucket always exists
    this.createBucket('default')

    this.logger.debug('database schema initialized')
  }

  // ── Bucket operations ─────────────────────────────────────────────

  createBucket(name: string): boolean {
    const stmt = this.db.prepare('INSERT OR IGNORE INTO buckets (name) VALUES (?)')
    const result = stmt.run(name)
    if (result.changes > 0) {
      this.logger.debug({ bucket: name }, 'bucket created')
    }
    return result.changes > 0
  }

  deleteBucket(name: string): boolean {
    // Don't allow deleting default bucket
    if (name === 'default') return false

    // Check if bucket has objects
    const hasObjects = this.db.prepare(
      'SELECT 1 FROM objects WHERE bucket = ? AND deleted = 0 LIMIT 1'
    ).get(name)
    if (hasObjects) return false

    const stmt = this.db.prepare('DELETE FROM buckets WHERE name = ?')
    const result = stmt.run(name)
    return result.changes > 0
  }

  listBuckets(): S3Bucket[] {
    const rows = this.db.prepare('SELECT name, created_at as creationDate FROM buckets ORDER BY name').all()
    return rows as S3Bucket[]
  }

  bucketExists(name: string): boolean {
    const row = this.db.prepare('SELECT 1 FROM buckets WHERE name = ?').get(name)
    return row !== undefined
  }

  // ── Object operations ──────────────────────────────────────────────

  putObject(options: PutObjectOptions): void
  putObject(bucket: string, key: string, pieceCid: string, size: number, contentType: string, etag: string, copies?: CopyInfo[]): void
  putObject(
    bucketOrOptions: string | PutObjectOptions,
    key?: string,
    pieceCid?: string,
    size?: number,
    contentType?: string,
    etag?: string,
    copies?: CopyInfo[]
  ): void {
    // Normalize arguments
    let bucket: string
    let k: string
    let pc: string
    let sz: number
    let ct: string
    let et: string
    let cp: CopyInfo[] | undefined

    if (typeof bucketOrOptions === 'object') {
      bucket = bucketOrOptions.bucket
      k = bucketOrOptions.key
      pc = bucketOrOptions.pieceCid
      sz = bucketOrOptions.size
      ct = bucketOrOptions.contentType
      et = bucketOrOptions.etag
      cp = bucketOrOptions.copies
    } else {
      bucket = bucketOrOptions
      k = key!
      pc = pieceCid!
      sz = size!
      ct = contentType!
      et = etag!
      cp = copies
    }

    const copiesCount = cp?.length ?? 0

    const putStmt = this.db.prepare(`
      INSERT INTO objects (bucket, key, piece_cid, size, content_type, etag, copies_count, updated_at, deleted)
      VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'), 0)
      ON CONFLICT (bucket, key) DO UPDATE SET
        piece_cid = excluded.piece_cid,
        size = excluded.size,
        content_type = excluded.content_type,
        etag = excluded.etag,
        copies_count = excluded.copies_count,
        updated_at = datetime('now'),
        deleted = 0
    `)

    const transaction = this.db.transaction(() => {
      putStmt.run(bucket, k, pc, sz, ct, et, copiesCount)

      // Replace copy records if provided
      if (cp && cp.length > 0) {
        this.db.prepare('DELETE FROM object_copies WHERE bucket = ? AND key = ?').run(bucket, k)

        const insertCopy = this.db.prepare(`
          INSERT INTO object_copies (bucket, key, provider_id, data_set_id, retrieval_url, role)
          VALUES (?, ?, ?, ?, ?, ?)
        `)
        for (const copy of cp) {
          insertCopy.run(bucket, k, copy.providerId, copy.dataSetId, copy.retrievalUrl, copy.role)
        }
      }
    })

    transaction()
    this.logger.debug({ bucket, key: k, pieceCid: pc, copies: copiesCount }, 'object stored')
  }

  getObject(bucket: string, key: string): S3Object | undefined {
    const stmt = this.db.prepare(`
      SELECT key, size, updated_at as lastModified, etag, piece_cid as pieceCid, content_type as contentType
      FROM objects
      WHERE bucket = ? AND key = ? AND deleted = 0
    `)
    const row = stmt.get(bucket, key) as S3Object | undefined
    return row
  }

  /** Get copy records for an object — used for choosing download source */
  getObjectCopies(bucket: string, key: string): CopyInfo[] {
    const rows = this.db.prepare(`
      SELECT provider_id as providerId, data_set_id as dataSetId, retrieval_url as retrievalUrl, role
      FROM object_copies
      WHERE bucket = ? AND key = ?
      ORDER BY role ASC
    `).all(bucket, key)
    return rows as CopyInfo[]
  }

  listObjects(
    bucket: string,
    prefix: string,
    delimiter: string,
    maxKeys: number,
    startAfter?: string
  ): { objects: S3Object[]; commonPrefixes: string[]; isTruncated: boolean } {
    let query = `
      SELECT key, size, updated_at as lastModified, etag, piece_cid as pieceCid, content_type as contentType
      FROM objects
      WHERE bucket = ? AND deleted = 0
    `
    const params: (string | number)[] = [bucket]

    if (prefix) {
      query += ' AND key LIKE ?'
      params.push(`${prefix}%`)
    }
    if (startAfter) {
      query += ' AND key > ?'
      params.push(startAfter)
    }

    query += ' ORDER BY key ASC LIMIT ?'
    params.push(maxKeys + 1)

    const rows = this.db.prepare(query).all(...params) as S3Object[]

    const isTruncated = rows.length > maxKeys
    const objects = isTruncated ? rows.slice(0, maxKeys) : rows

    // Handle delimiter-based common prefix grouping
    const commonPrefixes: string[] = []
    const filteredObjects: S3Object[] = []

    if (delimiter) {
      const seenPrefixes = new Set<string>()
      for (const obj of objects) {
        const keyAfterPrefix = obj.key.slice(prefix.length)
        const delimiterIndex = keyAfterPrefix.indexOf(delimiter)
        if (delimiterIndex >= 0) {
          const commonPrefix = prefix + keyAfterPrefix.slice(0, delimiterIndex + delimiter.length)
          if (!seenPrefixes.has(commonPrefix)) {
            seenPrefixes.add(commonPrefix)
            commonPrefixes.push(commonPrefix)
          }
        } else {
          filteredObjects.push(obj)
        }
      }
      return { objects: filteredObjects, commonPrefixes, isTruncated }
    }

    return { objects, commonPrefixes, isTruncated }
  }

  deleteObject(bucket: string, key: string): boolean {
    const stmt = this.db.prepare(`
      UPDATE objects SET deleted = 1, updated_at = datetime('now')
      WHERE bucket = ? AND key = ? AND deleted = 0
    `)
    const result = stmt.run(bucket, key)
    return result.changes > 0
  }

  /**
   * Copy an object from one location to another (same or cross-bucket).
   * Both source and destination point to the same PieceCID — no data is re-uploaded.
   * Returns the copied object, or undefined if source doesn't exist.
   */
  copyObject(srcBucket: string, srcKey: string, dstBucket: string, dstKey: string): S3Object | undefined {
    const src = this.getObject(srcBucket, srcKey)
    if (!src) return undefined

    const srcCopies = this.getObjectCopies(srcBucket, srcKey)

    const transaction = this.db.transaction(() => {
      // Insert/replace destination object with same pieceCid
      this.db.prepare(`
        INSERT INTO objects (bucket, key, piece_cid, size, content_type, etag, copies_count, updated_at, deleted)
        VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'), 0)
        ON CONFLICT (bucket, key) DO UPDATE SET
          piece_cid = excluded.piece_cid,
          size = excluded.size,
          content_type = excluded.content_type,
          etag = excluded.etag,
          copies_count = excluded.copies_count,
          updated_at = datetime('now'),
          deleted = 0
      `).run(dstBucket, dstKey, src.pieceCid, src.size, src.contentType, src.etag, srcCopies.length)

      // Copy provider records
      if (srcCopies.length > 0) {
        this.db.prepare('DELETE FROM object_copies WHERE bucket = ? AND key = ?').run(dstBucket, dstKey)
        const insertCopy = this.db.prepare(`
          INSERT INTO object_copies (bucket, key, provider_id, data_set_id, retrieval_url, role)
          VALUES (?, ?, ?, ?, ?, ?)
        `)
        for (const copy of srcCopies) {
          insertCopy.run(dstBucket, dstKey, copy.providerId, copy.dataSetId, copy.retrievalUrl, copy.role)
        }
      }
    })

    transaction()
    this.logger.debug({ srcBucket, srcKey, dstBucket, dstKey, pieceCid: src.pieceCid }, 'object copied')

    return this.getObject(dstBucket, dstKey)!
  }

  objectExists(bucket: string, key: string): boolean {
    const stmt = this.db.prepare(
      'SELECT 1 FROM objects WHERE bucket = ? AND key = ? AND deleted = 0'
    )
    return stmt.get(bucket, key) !== undefined
  }

  close(): void {
    this.db.close()
  }
}
