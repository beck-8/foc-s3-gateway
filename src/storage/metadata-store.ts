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
        status TEXT NOT NULL DEFAULT 'uploaded',
        local_path TEXT,
        upload_attempts INTEGER NOT NULL DEFAULT 0,
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
        piece_id TEXT NOT NULL,
        retrieval_url TEXT NOT NULL,
        role TEXT NOT NULL DEFAULT 'primary',
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        PRIMARY KEY (bucket, key, provider_id)
      );

      CREATE TABLE IF NOT EXISTS pending_deletions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        piece_cid TEXT NOT NULL,
        piece_id TEXT NOT NULL,
        provider_id TEXT NOT NULL,
        data_set_id TEXT NOT NULL,
        retrieval_url TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        last_attempt TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
      );

      CREATE TABLE IF NOT EXISTS multipart_uploads (
        upload_id TEXT PRIMARY KEY,
        bucket TEXT NOT NULL,
        key TEXT NOT NULL,
        content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
      );

      CREATE TABLE IF NOT EXISTS multipart_parts (
        upload_id TEXT NOT NULL,
        part_number INTEGER NOT NULL,
        local_path TEXT NOT NULL,
        size INTEGER NOT NULL,
        etag TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        PRIMARY KEY (upload_id, part_number)
      );

      CREATE TABLE IF NOT EXISTS config (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
      );

      CREATE INDEX IF NOT EXISTS idx_objects_bucket ON objects(bucket);
      CREATE INDEX IF NOT EXISTS idx_objects_prefix ON objects(bucket, key);
      CREATE INDEX IF NOT EXISTS idx_objects_piece_cid ON objects(piece_cid);
      CREATE INDEX IF NOT EXISTS idx_copies_piece ON object_copies(bucket, key);
      CREATE INDEX IF NOT EXISTS idx_pending_piece_cid ON pending_deletions(piece_cid);
      CREATE INDEX IF NOT EXISTS idx_multipart_bucket ON multipart_uploads(bucket, key);
    `)

    // Migrate existing databases: add new columns if missing
    // MUST run before creating indexes on new columns (e.g. status)
    this.migrateSchema()

    // Indexes on columns added by migration (safe to create after migrateSchema)
    this.db.exec(`
      CREATE INDEX IF NOT EXISTS idx_objects_status ON objects(status);
    `)

    // Ensure default bucket always exists
    this.createBucket('default')

    this.logger.debug('database schema initialized')
  }

  /** Add columns that may be missing in databases created before async upload support */
  private migrateSchema(): void {
    const columns = this.db.prepare("SELECT name FROM pragma_table_info('objects')").all() as Array<{ name: string }>
    const colNames = new Set(columns.map((c) => c.name))

    if (!colNames.has('status')) {
      this.db.exec("ALTER TABLE objects ADD COLUMN status TEXT NOT NULL DEFAULT 'uploaded'")
      this.logger.info('migrated: added status column to objects')
    }
    if (!colNames.has('local_path')) {
      this.db.exec('ALTER TABLE objects ADD COLUMN local_path TEXT')
      this.logger.info('migrated: added local_path column to objects')
    }
    if (!colNames.has('upload_attempts')) {
      this.db.exec('ALTER TABLE objects ADD COLUMN upload_attempts INTEGER NOT NULL DEFAULT 0')
      this.logger.info('migrated: added upload_attempts column to objects')
    }
  }

  // ── Config / Identity ─────────────────────────────────────────────

  /** Get a config value by key */
  getConfig(key: string): string | undefined {
    const row = this.db.prepare('SELECT value FROM config WHERE key = ?').get(key) as { value: string } | undefined
    return row?.value
  }

  /** Set a config value */
  setConfig(key: string, value: string): void {
    this.db.prepare('INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)').run(key, value)
  }

  /**
   * Validate that the wallet address matches the one stored in the database.
   * On first startup, stores the address. On subsequent startups, verifies it matches.
   * Throws if the address doesn't match — prevents accidental key changes.
   */
  validateWalletAddress(address: string): void {
    const stored = this.getConfig('wallet_address')

    if (!stored) {
      // First startup — store the address
      this.setConfig('wallet_address', address.toLowerCase())
      this.logger.info({ address }, 'wallet address bound to this database')
      return
    }

    if (stored.toLowerCase() !== address.toLowerCase()) {
      throw new Error(
        `Wallet address mismatch!\n` +
          `  Database is bound to: ${stored}\n` +
          `  Current PRIVATE_KEY:  ${address}\n\n` +
          `All data sets and uploads are tied to the original wallet.\n` +
          `Changing PRIVATE_KEY will break uploads, downloads, and deletions.\n` +
          `If you intentionally want to reset, delete the database file and restart.`
      )
    }

    this.logger.debug({ address }, 'wallet address verified')
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
    const hasObjects = this.db.prepare('SELECT 1 FROM objects WHERE bucket = ? AND deleted = 0 LIMIT 1').get(name)
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
  putObject(
    bucket: string,
    key: string,
    pieceCid: string,
    size: number,
    contentType: string,
    etag: string,
    copies?: CopyInfo[]
  ): void
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
      INSERT INTO objects (bucket, key, piece_cid, size, content_type, etag, copies_count, status, local_path, upload_attempts, updated_at, deleted)
      VALUES (?, ?, ?, ?, ?, ?, ?, 'uploaded', NULL, 0, datetime('now'), 0)
      ON CONFLICT (bucket, key) DO UPDATE SET
        piece_cid = excluded.piece_cid,
        size = excluded.size,
        content_type = excluded.content_type,
        etag = excluded.etag,
        copies_count = excluded.copies_count,
        status = 'uploaded',
        local_path = NULL,
        upload_attempts = 0,
        updated_at = datetime('now'),
        deleted = 0
    `)

    const transaction = this.db.transaction(() => {
      // Clean up old copies if overwriting an existing object with different pieceCid
      const existing = this.db
        .prepare('SELECT piece_cid FROM objects WHERE bucket = ? AND key = ? AND deleted = 0')
        .get(bucket, k) as { piece_cid: string } | undefined

      if (existing?.piece_cid && existing.piece_cid !== pc) {
        const oldCopies = this.db
          .prepare(
            'SELECT provider_id, data_set_id, piece_id, retrieval_url FROM object_copies WHERE bucket = ? AND key = ?'
          )
          .all(bucket, k) as Array<{
          provider_id: string
          data_set_id: string
          piece_id: string
          retrieval_url: string
        }>

        // Check if any OTHER object still references the old pieceCid
        const otherRef = this.db
          .prepare('SELECT 1 FROM objects WHERE piece_cid = ? AND deleted = 0 AND NOT (bucket = ? AND key = ?) LIMIT 1')
          .get(existing.piece_cid, bucket, k)

        if (!otherRef && oldCopies.length > 0) {
          const insertPending = this.db.prepare(`
            INSERT INTO pending_deletions (piece_cid, piece_id, provider_id, data_set_id, retrieval_url)
            VALUES (?, ?, ?, ?, ?)
          `)
          for (const copy of oldCopies) {
            insertPending.run(existing.piece_cid, copy.piece_id, copy.provider_id, copy.data_set_id, copy.retrieval_url)
          }
          this.logger.info(
            { bucket, key: k, oldPieceCid: existing.piece_cid, pendingCount: oldCopies.length },
            'old copies queued for SP cleanup (putObject overwrite)'
          )
        }
      }

      putStmt.run(bucket, k, pc, sz, ct, et, copiesCount)

      // Replace copy records if provided
      if (cp && cp.length > 0) {
        this.db.prepare('DELETE FROM object_copies WHERE bucket = ? AND key = ?').run(bucket, k)

        const insertCopy = this.db.prepare(`
          INSERT INTO object_copies (bucket, key, provider_id, data_set_id, piece_id, retrieval_url, role)
          VALUES (?, ?, ?, ?, ?, ?, ?)
        `)
        for (const copy of cp) {
          insertCopy.run(bucket, k, copy.providerId, copy.dataSetId, copy.pieceId, copy.retrievalUrl, copy.role)
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
    const rows = this.db
      .prepare(`
      SELECT provider_id as providerId, data_set_id as dataSetId, piece_id as pieceId, retrieval_url as retrievalUrl, role
      FROM object_copies
      WHERE bucket = ? AND key = ?
      ORDER BY role ASC
    `)
      .all(bucket, key)
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
    let deleted = false

    const transaction = this.db.transaction(() => {
      // Get the object's piece_cid and copies before deleting
      const obj = this.db
        .prepare('SELECT piece_cid FROM objects WHERE bucket = ? AND key = ? AND deleted = 0')
        .get(bucket, key) as { piece_cid: string } | undefined

      if (!obj) return

      const copies = this.db
        .prepare(
          'SELECT provider_id, data_set_id, piece_id, retrieval_url FROM object_copies WHERE bucket = ? AND key = ?'
        )
        .all(bucket, key) as Array<{
        provider_id: string
        data_set_id: string
        piece_id: string
        retrieval_url: string
      }>

      // Soft-delete the object
      this.db
        .prepare(`UPDATE objects SET deleted = 1, updated_at = datetime('now') WHERE bucket = ? AND key = ?`)
        .run(bucket, key)

      // Remove copy records
      this.db.prepare('DELETE FROM object_copies WHERE bucket = ? AND key = ?').run(bucket, key)

      // Check if any other non-deleted object still references this piece_cid
      const otherRef = this.db
        .prepare('SELECT 1 FROM objects WHERE piece_cid = ? AND deleted = 0 LIMIT 1')
        .get(obj.piece_cid)

      // No other references -- queue copies for SP cleanup
      if (!otherRef && copies.length > 0) {
        const insertPending = this.db.prepare(`
          INSERT INTO pending_deletions (piece_cid, piece_id, provider_id, data_set_id, retrieval_url)
          VALUES (?, ?, ?, ?, ?)
        `)
        for (const copy of copies) {
          insertPending.run(obj.piece_cid, copy.piece_id, copy.provider_id, copy.data_set_id, copy.retrieval_url)
        }
        this.logger.info(
          { bucket, key, pieceCid: obj.piece_cid, pendingCount: copies.length },
          'piece queued for SP cleanup (no remaining references)'
        )
      }

      deleted = true
    })

    transaction()
    return deleted
  }

  // -- Pending deletion helpers (used by cleanup worker) --

  /** Pending deletion record for the cleanup worker */
  getPendingDeletions(limit = 10): Array<{
    id: number
    piece_cid: string
    piece_id: string
    provider_id: string
    data_set_id: string
    retrieval_url: string
    attempts: number
  }> {
    return this.db
      .prepare(`
      SELECT id, piece_cid, piece_id, provider_id, data_set_id, retrieval_url, attempts
      FROM pending_deletions
      WHERE attempts < 5 
        AND (last_attempt IS NULL OR last_attempt < datetime('now', '-5 minutes'))
      ORDER BY created_at ASC
      LIMIT ?
    `)
      .all(limit) as any
  }

  /** Remove a pending deletion after successful SP cleanup */
  removePendingDeletion(id: number): void {
    this.db.prepare('DELETE FROM pending_deletions WHERE id = ?').run(id)
  }

  /** Increment attempt counter after a failed cleanup try */
  incrementDeletionAttempt(id: number): void {
    this.db
      .prepare(`
      UPDATE pending_deletions SET attempts = attempts + 1, last_attempt = datetime('now')
      WHERE id = ?
    `)
      .run(id)
  }

  /** Get deletion queue statistics for the status API */
  getDeletionStats(): { pending: number; failed: number; total: number } {
    const stats = this.db
      .prepare(`
      SELECT
        COUNT(*) as total,
        SUM(CASE WHEN attempts < 5 THEN 1 ELSE 0 END) as pending,
        SUM(CASE WHEN attempts >= 5 THEN 1 ELSE 0 END) as failed
      FROM pending_deletions
    `)
      .get() as { total: number; pending: number; failed: number }

    return {
      pending: stats.pending ?? 0,
      failed: stats.failed ?? 0,
      total: stats.total ?? 0,
    }
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
      // Clean up old copies if destination exists with a different pieceCid
      const existing = this.db
        .prepare('SELECT piece_cid FROM objects WHERE bucket = ? AND key = ? AND deleted = 0')
        .get(dstBucket, dstKey) as { piece_cid: string } | undefined

      if (existing?.piece_cid && existing.piece_cid !== src.pieceCid) {
        const oldCopies = this.db
          .prepare(
            'SELECT provider_id, data_set_id, piece_id, retrieval_url FROM object_copies WHERE bucket = ? AND key = ?'
          )
          .all(dstBucket, dstKey) as Array<{
          provider_id: string
          data_set_id: string
          piece_id: string
          retrieval_url: string
        }>

        const otherRef = this.db
          .prepare(
            'SELECT 1 FROM objects WHERE piece_cid = ? AND deleted = 0 AND NOT (bucket = ? AND key = ?) LIMIT 1'
          )
          .get(existing.piece_cid, dstBucket, dstKey)

        if (!otherRef && oldCopies.length > 0) {
          const insertPending = this.db.prepare(`
            INSERT INTO pending_deletions (piece_cid, piece_id, provider_id, data_set_id, retrieval_url)
            VALUES (?, ?, ?, ?, ?)
          `)
          for (const copy of oldCopies) {
            insertPending.run(existing.piece_cid, copy.piece_id, copy.provider_id, copy.data_set_id, copy.retrieval_url)
          }
          this.logger.info(
            { dstBucket, dstKey, oldPieceCid: existing.piece_cid, pendingCount: oldCopies.length },
            'old copies queued for SP cleanup (copyObject overwrite)'
          )
        }
      }

      // Insert/replace destination object with same pieceCid
      this.db
        .prepare(`
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
        .run(dstBucket, dstKey, src.pieceCid, src.size, src.contentType, src.etag, srcCopies.length)

      // Copy provider records
      if (srcCopies.length > 0) {
        this.db.prepare('DELETE FROM object_copies WHERE bucket = ? AND key = ?').run(dstBucket, dstKey)
        const insertCopy = this.db.prepare(`
          INSERT INTO object_copies (bucket, key, provider_id, data_set_id, piece_id, retrieval_url, role)
          VALUES (?, ?, ?, ?, ?, ?, ?)
        `)
        for (const copy of srcCopies) {
          insertCopy.run(dstBucket, dstKey, copy.providerId, copy.dataSetId, copy.pieceId, copy.retrievalUrl, copy.role)
        }
      }
    })

    transaction()
    this.logger.debug({ srcBucket, srcKey, dstBucket, dstKey, pieceCid: src.pieceCid }, 'object copied')

    return this.getObject(dstBucket, dstKey)!
  }

  objectExists(bucket: string, key: string): boolean {
    const stmt = this.db.prepare('SELECT 1 FROM objects WHERE bucket = ? AND key = ? AND deleted = 0')
    return stmt.get(bucket, key) !== undefined
  }

  // ── Async upload operations ─────────────────────────────────────────

  /** Stage an object for async FOC upload (returns immediately, worker uploads later) */
  stageObject(bucket: string, key: string, size: number, contentType: string, etag: string, localPath: string): void {
    const transaction = this.db.transaction(() => {
      // Check if an existing object has FOC copies that need cleanup
      const existing = this.db
        .prepare('SELECT piece_cid FROM objects WHERE bucket = ? AND key = ? AND deleted = 0')
        .get(bucket, key) as { piece_cid: string } | undefined

      if (existing?.piece_cid) {
        const copies = this.db
          .prepare(
            'SELECT provider_id, data_set_id, piece_id, retrieval_url FROM object_copies WHERE bucket = ? AND key = ?'
          )
          .all(bucket, key) as Array<{
          provider_id: string
          data_set_id: string
          piece_id: string
          retrieval_url: string
        }>

        // Remove old copy records
        this.db.prepare('DELETE FROM object_copies WHERE bucket = ? AND key = ?').run(bucket, key)

        // Check if any OTHER non-deleted object still references this piece_cid
        const otherRef = this.db
          .prepare('SELECT 1 FROM objects WHERE piece_cid = ? AND deleted = 0 AND NOT (bucket = ? AND key = ?) LIMIT 1')
          .get(existing.piece_cid, bucket, key)

        // No other references — queue copies for SP cleanup
        if (!otherRef && copies.length > 0) {
          const insertPending = this.db.prepare(`
            INSERT INTO pending_deletions (piece_cid, piece_id, provider_id, data_set_id, retrieval_url)
            VALUES (?, ?, ?, ?, ?)
          `)
          for (const copy of copies) {
            insertPending.run(existing.piece_cid, copy.piece_id, copy.provider_id, copy.data_set_id, copy.retrieval_url)
          }
          this.logger.info(
            { bucket, key, pieceCid: existing.piece_cid, pendingCount: copies.length },
            'old copies queued for SP cleanup (overwrite)'
          )
        }
      }

      // Upsert the new object
      this.db
        .prepare(
          `INSERT INTO objects (bucket, key, piece_cid, size, content_type, etag, copies_count, status, local_path, upload_attempts, updated_at, deleted)
           VALUES (?, ?, '', ?, ?, ?, 0, 'pending', ?, 0, datetime('now'), 0)
           ON CONFLICT (bucket, key) DO UPDATE SET
             piece_cid = '',
             size = excluded.size,
             content_type = excluded.content_type,
             etag = excluded.etag,
             copies_count = 0,
             status = 'pending',
             local_path = excluded.local_path,
             upload_attempts = 0,
             updated_at = datetime('now'),
             deleted = 0`
        )
        .run(bucket, key, size, contentType, etag, localPath)
    })

    transaction()
    this.logger.debug({ bucket, key, size, localPath }, 'object staged for async upload')
  }

  /** Get objects waiting to be uploaded to FOC */
  getPendingUploads(limit = 5): Array<{
    bucket: string
    key: string
    size: number
    contentType: string
    localPath: string
  }> {
    return this.db
      .prepare(
        `SELECT bucket, key, size, content_type as contentType, local_path as localPath
         FROM objects
         WHERE status IN ('pending', 'failed') AND deleted = 0
           AND upload_attempts < 10
         ORDER BY updated_at ASC
         LIMIT ?`
      )
      .all(limit) as any
  }

  /** Mark an object as currently being uploaded */
  markUploading(bucket: string, key: string): void {
    this.db
      .prepare(
        `UPDATE objects SET status = 'uploading', upload_attempts = upload_attempts + 1, updated_at = datetime('now')
         WHERE bucket = ? AND key = ? AND deleted = 0`
      )
      .run(bucket, key)
  }

  /** Complete an async upload: set pieceCid, copies, clear local_path */
  completeUpload(bucket: string, key: string, pieceCid: string, copies?: CopyInfo[]): void {
    const copiesCount = copies?.length ?? 0

    const transaction = this.db.transaction(() => {
      this.db
        .prepare(
          `UPDATE objects SET piece_cid = ?, copies_count = ?, status = 'uploaded', local_path = NULL, updated_at = datetime('now')
           WHERE bucket = ? AND key = ? AND deleted = 0`
        )
        .run(pieceCid, copiesCount, bucket, key)

      if (copies && copies.length > 0) {
        this.db.prepare('DELETE FROM object_copies WHERE bucket = ? AND key = ?').run(bucket, key)
        const insertCopy = this.db.prepare(
          `INSERT INTO object_copies (bucket, key, provider_id, data_set_id, piece_id, retrieval_url, role)
           VALUES (?, ?, ?, ?, ?, ?, ?)`
        )
        for (const copy of copies) {
          insertCopy.run(bucket, key, copy.providerId, copy.dataSetId, copy.pieceId, copy.retrievalUrl, copy.role)
        }
      }
    })

    transaction()
    this.logger.debug({ bucket, key, pieceCid, copies: copiesCount }, 'upload completed')
  }

  /** Mark an upload as failed (will be retried by worker) */
  markUploadFailed(bucket: string, key: string): void {
    this.db
      .prepare(
        `UPDATE objects SET status = 'failed', updated_at = datetime('now')
         WHERE bucket = ? AND key = ? AND deleted = 0`
      )
      .run(bucket, key)
  }

  /** Get the local path for an object (if it's still staged on disk) */
  getLocalPath(bucket: string, key: string): string | undefined {
    const row = this.db
      .prepare('SELECT local_path FROM objects WHERE bucket = ? AND key = ? AND deleted = 0')
      .get(bucket, key) as { local_path: string | null } | undefined
    return row?.local_path ?? undefined
  }

  /** Get all local paths that are still referenced (for orphan cleanup) */
  getAllLocalPaths(): Set<string> {
    const rows = this.db
      .prepare('SELECT local_path FROM objects WHERE local_path IS NOT NULL AND deleted = 0')
      .all() as Array<{ local_path: string }>
    return new Set(rows.map((r) => r.local_path))
  }

  // ── Multipart upload operations ─────────────────────────────────────

  /** Create a new multipart upload session */
  createMultipartUpload(uploadId: string, bucket: string, key: string, contentType: string): void {
    this.db
      .prepare('INSERT INTO multipart_uploads (upload_id, bucket, key, content_type) VALUES (?, ?, ?, ?)')
      .run(uploadId, bucket, key, contentType)
    this.logger.debug({ uploadId, bucket, key }, 'multipart upload created')
  }

  /** Get a multipart upload session */
  getMultipartUpload(
    uploadId: string
  ): { uploadId: string; bucket: string; key: string; contentType: string } | undefined {
    return this.db
      .prepare(
        'SELECT upload_id as uploadId, bucket, key, content_type as contentType FROM multipart_uploads WHERE upload_id = ?'
      )
      .get(uploadId) as any
  }

  /** Record a completed part */
  addMultipartPart(uploadId: string, partNumber: number, localPath: string, size: number, etag: string): void {
    this.db
      .prepare(
        `INSERT INTO multipart_parts (upload_id, part_number, local_path, size, etag)
         VALUES (?, ?, ?, ?, ?)
         ON CONFLICT (upload_id, part_number) DO UPDATE SET
           local_path = excluded.local_path,
           size = excluded.size,
           etag = excluded.etag`
      )
      .run(uploadId, partNumber, localPath, size, etag)
  }

  /** Get all parts for a multipart upload, sorted by part number */
  getMultipartParts(uploadId: string): Array<{ partNumber: number; localPath: string; size: number; etag: string }> {
    return this.db
      .prepare(
        `SELECT part_number as partNumber, local_path as localPath, size, etag
         FROM multipart_parts WHERE upload_id = ? ORDER BY part_number ASC`
      )
      .all(uploadId) as any
  }

  /** Delete a multipart upload session and its parts records */
  deleteMultipartUpload(uploadId: string): void {
    const transaction = this.db.transaction(() => {
      this.db.prepare('DELETE FROM multipart_parts WHERE upload_id = ?').run(uploadId)
      this.db.prepare('DELETE FROM multipart_uploads WHERE upload_id = ?').run(uploadId)
    })
    transaction()
    this.logger.debug({ uploadId }, 'multipart upload deleted')
  }

  /** List active multipart uploads for a bucket */
  listMultipartUploads(
    bucket: string
  ): Array<{ uploadId: string; key: string; contentType: string; createdAt: string }> {
    return this.db
      .prepare(
        `SELECT upload_id as uploadId, key, content_type as contentType, created_at as createdAt
         FROM multipart_uploads WHERE bucket = ? ORDER BY created_at ASC`
      )
      .all(bucket) as any
  }

  /** Count all active multipart uploads across all buckets */
  countAllMultipartUploads(): number {
    const row = this.db.prepare('SELECT COUNT(*) as count FROM multipart_uploads').get() as { count: number }
    return row.count
  }

  // ── Status / stats ──────────────────────────────────────────────────

  /** Get aggregate upload status counts */
  getUploadStats(): { pending: number; uploading: number; uploaded: number; failed: number } {
    const rows = this.db
      .prepare(
        `SELECT status, COUNT(*) as count
         FROM objects WHERE deleted = 0
         GROUP BY status`
      )
      .all() as Array<{ status: string; count: number }>

    const stats = { pending: 0, uploading: 0, uploaded: 0, failed: 0 }
    for (const row of rows) {
      if (row.status in stats) {
        stats[row.status as keyof typeof stats] = row.count
      }
    }
    return stats
  }

  /** Get objects by status (for detailed status page) */
  getObjectsByStatus(
    status: string,
    limit = 50
  ): Array<{ bucket: string; key: string; size: number; uploadAttempts: number; updatedAt: string }> {
    return this.db
      .prepare(
        `SELECT bucket, key, size, upload_attempts as uploadAttempts, updated_at as updatedAt
         FROM objects WHERE status = ? AND deleted = 0
         ORDER BY updated_at DESC LIMIT ?`
      )
      .all(status, limit) as any
  }

  close(): void {
    this.db.close()
  }
}
