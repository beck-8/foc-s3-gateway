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
  desiredCopies?: number | undefined
  copies?: CopyInfo[] | undefined
}

export class MetadataStore {
  private readonly db: Database.Database
  private readonly logger: Logger
  private static readonly DEFAULT_DESIRED_COPIES_KEY = 'default_desired_copies'

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
        desired_copies INTEGER NOT NULL DEFAULT 2,
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
        health_status TEXT NOT NULL DEFAULT 'healthy',
        consecutive_failures INTEGER NOT NULL DEFAULT 0,
        last_checked_at TEXT,
        last_success_at TEXT,
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
      CREATE INDEX IF NOT EXISTS idx_copies_last_checked ON object_copies(last_checked_at, bucket, key);
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
    if (!colNames.has('desired_copies')) {
      this.db.exec('ALTER TABLE objects ADD COLUMN desired_copies INTEGER NOT NULL DEFAULT 2')
      this.logger.info('migrated: added desired_copies column to objects')
    }

    const copyColumns = this.db.prepare("SELECT name FROM pragma_table_info('object_copies')").all() as Array<{
      name: string
    }>
    const copyColNames = new Set(copyColumns.map((c) => c.name))
    if (!copyColNames.has('health_status')) {
      this.db.exec("ALTER TABLE object_copies ADD COLUMN health_status TEXT NOT NULL DEFAULT 'healthy'")
      this.db.exec(
        "UPDATE object_copies SET health_status = 'healthy' WHERE health_status IS NULL OR health_status = ''"
      )
      this.logger.info('migrated: added health_status column to object_copies')
    }
    if (!copyColNames.has('consecutive_failures')) {
      this.db.exec('ALTER TABLE object_copies ADD COLUMN consecutive_failures INTEGER NOT NULL DEFAULT 0')
      this.logger.info('migrated: added consecutive_failures column to object_copies')
    }
    if (!copyColNames.has('last_checked_at')) {
      this.db.exec('ALTER TABLE object_copies ADD COLUMN last_checked_at TEXT')
      this.db.exec("UPDATE object_copies SET last_checked_at = datetime('now') WHERE last_checked_at IS NULL")
      this.logger.info('migrated: added last_checked_at column to object_copies')
    }
    if (!copyColNames.has('last_success_at')) {
      this.db.exec('ALTER TABLE object_copies ADD COLUMN last_success_at TEXT')
      this.db.exec("UPDATE object_copies SET last_success_at = datetime('now') WHERE last_success_at IS NULL")
      this.logger.info('migrated: added last_success_at column to object_copies')
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

  setDefaultDesiredCopies(copies: number): void {
    const value = this.normalizeDesiredCopies(copies)
    this.setConfig(MetadataStore.DEFAULT_DESIRED_COPIES_KEY, String(value))
  }

  private getDefaultDesiredCopies(): number {
    const raw = this.getConfig(MetadataStore.DEFAULT_DESIRED_COPIES_KEY)
    if (!raw) return 2
    const parsed = Number.parseInt(raw, 10)
    if (!Number.isFinite(parsed) || parsed < 1) return 2
    return parsed
  }

  private normalizeDesiredCopies(copies: number): number {
    if (!Number.isInteger(copies) || copies < 1) {
      throw new Error(`desired copies must be an integer >= 1 (received: ${copies})`)
    }
    return copies
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

    // Refuse deletion while multipart sessions still target this bucket
    const hasMultipartUploads = this.db.prepare('SELECT 1 FROM multipart_uploads WHERE bucket = ? LIMIT 1').get(name)
    if (hasMultipartUploads) return false

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
    let dc: number | undefined
    let cp: CopyInfo[] | undefined

    if (typeof bucketOrOptions === 'object') {
      bucket = bucketOrOptions.bucket
      k = bucketOrOptions.key
      pc = bucketOrOptions.pieceCid
      sz = bucketOrOptions.size
      ct = bucketOrOptions.contentType
      et = bucketOrOptions.etag
      dc = bucketOrOptions.desiredCopies
      cp = bucketOrOptions.copies
    } else {
      if (
        key === undefined ||
        pieceCid === undefined ||
        size === undefined ||
        contentType === undefined ||
        etag === undefined
      ) {
        throw new Error('putObject requires key, pieceCid, size, contentType, and etag')
      }
      bucket = bucketOrOptions
      k = key
      pc = pieceCid
      sz = size
      ct = contentType
      et = etag
      dc = undefined
      cp = copies
    }

    const desiredCopies = this.normalizeDesiredCopies(dc ?? this.getDefaultDesiredCopies())
    const copiesCount = cp?.length ?? 0

    const putStmt = this.db.prepare(`
      INSERT INTO objects (bucket, key, piece_cid, size, content_type, etag, copies_count, desired_copies, status, local_path, upload_attempts, updated_at, deleted)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'uploaded', NULL, 0, datetime('now'), 0)
      ON CONFLICT (bucket, key) DO UPDATE SET
        piece_cid = excluded.piece_cid,
        size = excluded.size,
        content_type = excluded.content_type,
        etag = excluded.etag,
        copies_count = excluded.copies_count,
        desired_copies = excluded.desired_copies,
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

      putStmt.run(bucket, k, pc, sz, ct, et, copiesCount, desiredCopies)

      // Replace copy records if provided
      if (cp && cp.length > 0) {
        this.db.prepare('DELETE FROM object_copies WHERE bucket = ? AND key = ?').run(bucket, k)

        const insertCopy = this.db.prepare(`
          INSERT INTO object_copies (
            bucket, key, provider_id, data_set_id, piece_id, retrieval_url, role, health_status, consecutive_failures, last_checked_at, last_success_at
          )
          VALUES (?, ?, ?, ?, ?, ?, ?, 'healthy', 0, datetime('now'), datetime('now'))
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

  /** Count pending deletions that have exhausted all retry attempts */
  getAbandonedDeletionCount(): number {
    const row = this.db.prepare('SELECT COUNT(*) as count FROM pending_deletions WHERE attempts >= 5').get() as {
      count: number
    }
    return row.count
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
    if (!this.bucketExists(dstBucket)) return undefined

    // Fetch full source row including upload state fields (status, local_path, upload_attempts)
    const srcFull = this.db
      .prepare(
        'SELECT desired_copies, status, local_path, upload_attempts FROM objects WHERE bucket = ? AND key = ? AND deleted = 0'
      )
      .get(srcBucket, srcKey) as
      | { desired_copies: number; status: string | null; local_path: string | null; upload_attempts: number }
      | undefined
    const srcDesiredCopies = srcFull?.desired_copies ?? 2
    const srcStatus = srcFull?.status ?? 'uploaded'
    const srcLocalPath = srcFull?.local_path ?? null
    const srcUploadAttempts = srcFull?.upload_attempts ?? 0

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
          .prepare('SELECT 1 FROM objects WHERE piece_cid = ? AND deleted = 0 AND NOT (bucket = ? AND key = ?) LIMIT 1')
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

      // Insert/replace destination object — preserve upload state (status, local_path, upload_attempts)
      this.db
        .prepare(`
        INSERT INTO objects (bucket, key, piece_cid, size, content_type, etag, copies_count, desired_copies, status, local_path, upload_attempts, updated_at, deleted)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), 0)
        ON CONFLICT (bucket, key) DO UPDATE SET
          piece_cid = excluded.piece_cid,
          size = excluded.size,
          content_type = excluded.content_type,
          etag = excluded.etag,
          copies_count = excluded.copies_count,
          desired_copies = excluded.desired_copies,
          status = excluded.status,
          local_path = excluded.local_path,
          upload_attempts = excluded.upload_attempts,
          updated_at = datetime('now'),
          deleted = 0
      `)
        .run(
          dstBucket,
          dstKey,
          src.pieceCid,
          src.size,
          src.contentType,
          src.etag,
          srcCopies.length,
          srcDesiredCopies,
          srcStatus,
          srcLocalPath,
          srcUploadAttempts
        )

      // Transfer local file ownership: clear source local_path so only destination owns the file
      if (srcLocalPath) {
        this.db
          .prepare('UPDATE objects SET local_path = NULL WHERE bucket = ? AND key = ? AND deleted = 0')
          .run(srcBucket, srcKey)
      }

      // Copy provider records
      if (srcCopies.length > 0) {
        this.db.prepare('DELETE FROM object_copies WHERE bucket = ? AND key = ?').run(dstBucket, dstKey)
        const insertCopy = this.db.prepare(`
          INSERT INTO object_copies (
            bucket, key, provider_id, data_set_id, piece_id, retrieval_url, role, health_status, consecutive_failures, last_checked_at, last_success_at
          )
          VALUES (?, ?, ?, ?, ?, ?, ?, 'healthy', 0, datetime('now'), datetime('now'))
        `)
        for (const copy of srcCopies) {
          insertCopy.run(dstBucket, dstKey, copy.providerId, copy.dataSetId, copy.pieceId, copy.retrievalUrl, copy.role)
        }
      }
    })

    transaction()
    this.logger.debug({ srcBucket, srcKey, dstBucket, dstKey, pieceCid: src.pieceCid }, 'object copied')

    const copied = this.getObject(dstBucket, dstKey)
    if (!copied) {
      throw new Error('copyObject failed to load copied destination object')
    }
    return copied
  }

  objectExists(bucket: string, key: string): boolean {
    const stmt = this.db.prepare('SELECT 1 FROM objects WHERE bucket = ? AND key = ? AND deleted = 0')
    return stmt.get(bucket, key) !== undefined
  }

  // ── Async upload operations ─────────────────────────────────────────

  /** Stage an object for async FOC upload (returns immediately, worker uploads later) */
  stageObject(
    bucket: string,
    key: string,
    size: number,
    contentType: string,
    etag: string,
    localPath: string,
    desiredCopies?: number
  ): void {
    const snapshotDesiredCopies = this.normalizeDesiredCopies(desiredCopies ?? this.getDefaultDesiredCopies())

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
          `INSERT INTO objects (bucket, key, piece_cid, size, content_type, etag, copies_count, desired_copies, status, local_path, upload_attempts, updated_at, deleted)
           VALUES (?, ?, '', ?, ?, ?, 0, ?, 'pending', ?, 0, datetime('now'), 0)
           ON CONFLICT (bucket, key) DO UPDATE SET
             piece_cid = '',
             size = excluded.size,
             content_type = excluded.content_type,
             etag = excluded.etag,
             copies_count = 0,
             desired_copies = excluded.desired_copies,
             status = 'pending',
             local_path = excluded.local_path,
             upload_attempts = 0,
             updated_at = datetime('now'),
             deleted = 0`
        )
        .run(bucket, key, size, contentType, etag, snapshotDesiredCopies, localPath)
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
    desiredCopies: number
  }> {
    return this.db
      .prepare(
        `SELECT bucket, key, size, content_type as contentType, local_path as localPath, desired_copies as desiredCopies
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
  completeUpload(bucket: string, key: string, pieceCid: string, copies?: CopyInfo[], localPath?: string): void {
    const copiesCount = copies?.length ?? 0

    const transaction = this.db.transaction(() => {
      const result = this.db
        .prepare(
          `UPDATE objects SET piece_cid = ?, copies_count = ?, status = 'uploaded', local_path = NULL, updated_at = datetime('now')
           WHERE bucket = ? AND key = ? AND deleted = 0`
        )
        .run(pieceCid, copiesCount, bucket, key)

      // Race condition: object was renamed or deleted during upload.
      // Find the new owner by local_path (copyObject transfers local_path to the destination).
      if (result.changes === 0 && localPath) {
        const newOwner = this.db
          .prepare('SELECT bucket, key FROM objects WHERE local_path = ? AND deleted = 0 LIMIT 1')
          .get(localPath) as { bucket: string; key: string } | undefined

        if (newOwner) {
          this.logger.info(
            { originalBucket: bucket, originalKey: key, newBucket: newOwner.bucket, newKey: newOwner.key, pieceCid },
            'object was renamed during upload, transferring pieceCid to new key'
          )
          this.db
            .prepare(
              `UPDATE objects SET piece_cid = ?, copies_count = ?, status = 'uploaded', local_path = NULL, updated_at = datetime('now')
               WHERE bucket = ? AND key = ? AND deleted = 0`
            )
            .run(pieceCid, copiesCount, newOwner.bucket, newOwner.key)

          // Insert copies under the new key
          if (copies && copies.length > 0) {
            this.db.prepare('DELETE FROM object_copies WHERE bucket = ? AND key = ?').run(newOwner.bucket, newOwner.key)
            const insertCopy = this.db.prepare(
              `INSERT INTO object_copies (
                 bucket, key, provider_id, data_set_id, piece_id, retrieval_url, role, health_status, consecutive_failures, last_checked_at, last_success_at
               )
               VALUES (?, ?, ?, ?, ?, ?, ?, 'healthy', 0, datetime('now'), datetime('now'))`
            )
            for (const copy of copies) {
              insertCopy.run(
                newOwner.bucket,
                newOwner.key,
                copy.providerId,
                copy.dataSetId,
                copy.pieceId,
                copy.retrievalUrl,
                copy.role
              )
            }
          }
          return
        }

        this.logger.warn(
          { bucket, key, pieceCid, localPath },
          'upload completed but object was deleted and no renamed target found'
        )
        return
      }

      if (copies && copies.length > 0) {
        this.db.prepare('DELETE FROM object_copies WHERE bucket = ? AND key = ?').run(bucket, key)
        const insertCopy = this.db.prepare(
          `INSERT INTO object_copies (
             bucket, key, provider_id, data_set_id, piece_id, retrieval_url, role, health_status, consecutive_failures, last_checked_at, last_success_at
           )
           VALUES (?, ?, ?, ?, ?, ?, ?, 'healthy', 0, datetime('now'), datetime('now'))`
        )
        for (const copy of copies) {
          insertCopy.run(bucket, key, copy.providerId, copy.dataSetId, copy.pieceId, copy.retrievalUrl, copy.role)
        }
      }
    })

    transaction()
    this.logger.debug({ bucket, key, pieceCid, copies: copiesCount }, 'upload completed')
  }

  /** Record copies from a partial upload and keep object readable while awaiting repair */
  recordPartialUpload(bucket: string, key: string, pieceCid: string, copies: CopyInfo[], localPath?: string): void {
    const copiesCount = copies.length

    const transaction = this.db.transaction(() => {
      const result = this.db
        .prepare(
          `UPDATE objects
           SET piece_cid = ?, copies_count = ?, status = 'uploaded', local_path = ?, updated_at = datetime('now')
           WHERE bucket = ? AND key = ? AND deleted = 0`
        )
        .run(pieceCid, copiesCount, localPath ?? null, bucket, key)

      // Race condition: object was renamed or deleted during upload (same as completeUpload)
      if (result.changes === 0 && localPath) {
        const newOwner = this.db
          .prepare('SELECT bucket, key FROM objects WHERE local_path = ? AND deleted = 0 LIMIT 1')
          .get(localPath) as { bucket: string; key: string } | undefined

        if (newOwner) {
          this.logger.info(
            { originalBucket: bucket, originalKey: key, newBucket: newOwner.bucket, newKey: newOwner.key, pieceCid },
            'object was renamed during partial upload, transferring pieceCid to new key'
          )
          this.db
            .prepare(
              `UPDATE objects
               SET piece_cid = ?, copies_count = ?, status = 'uploaded', local_path = ?, updated_at = datetime('now')
               WHERE bucket = ? AND key = ? AND deleted = 0`
            )
            .run(pieceCid, copiesCount, localPath ?? null, newOwner.bucket, newOwner.key)

          this.db.prepare('DELETE FROM object_copies WHERE bucket = ? AND key = ?').run(newOwner.bucket, newOwner.key)
          const insertCopy = this.db.prepare(
            `INSERT INTO object_copies (
               bucket, key, provider_id, data_set_id, piece_id, retrieval_url, role, health_status, consecutive_failures, last_checked_at, last_success_at
             )
             VALUES (?, ?, ?, ?, ?, ?, ?, 'healthy', 0, datetime('now'), datetime('now'))`
          )
          for (const copy of copies) {
            insertCopy.run(
              newOwner.bucket,
              newOwner.key,
              copy.providerId,
              copy.dataSetId,
              copy.pieceId,
              copy.retrievalUrl,
              copy.role
            )
          }
          return
        }

        this.logger.warn(
          { bucket, key, pieceCid, localPath },
          'partial upload completed but object was deleted and no renamed target found'
        )
        return
      }

      this.db.prepare('DELETE FROM object_copies WHERE bucket = ? AND key = ?').run(bucket, key)
      const insertCopy = this.db.prepare(
        `INSERT INTO object_copies (
           bucket, key, provider_id, data_set_id, piece_id, retrieval_url, role, health_status, consecutive_failures, last_checked_at, last_success_at
         )
         VALUES (?, ?, ?, ?, ?, ?, ?, 'healthy', 0, datetime('now'), datetime('now'))`
      )
      for (const copy of copies) {
        insertCopy.run(bucket, key, copy.providerId, copy.dataSetId, copy.pieceId, copy.retrievalUrl, copy.role)
      }
    })

    transaction()
    this.logger.warn({ bucket, key, pieceCid, copies: copiesCount }, 'partial upload recorded, waiting for repair')
  }

  getUnderReplicatedObjects(limit = 5): Array<{
    bucket: string
    key: string
    pieceCid: string
    desiredCopies: number
    copiesCount: number
    healthyCopies: number
  }> {
    return this.db
      .prepare(
        `SELECT
           o.bucket,
           o.key,
           o.piece_cid as pieceCid,
           o.desired_copies as desiredCopies,
           o.copies_count as copiesCount,
           COALESCE(h.healthyCopies, 0) as healthyCopies
         FROM objects o
         LEFT JOIN (
           SELECT bucket, key, COUNT(*) as healthyCopies
           FROM object_copies
           WHERE health_status = 'healthy'
           GROUP BY bucket, key
         ) h ON h.bucket = o.bucket AND h.key = o.key
         WHERE o.deleted = 0
           AND o.status = 'uploaded'
           AND o.piece_cid != ''
           AND COALESCE(h.healthyCopies, 0) < o.desired_copies
         ORDER BY o.updated_at ASC
         LIMIT ?`
      )
      .all(limit) as any
  }

  getUnderReplicatedCount(): number {
    const row = this.db
      .prepare(
        `SELECT COUNT(*) as count
         FROM objects o
         LEFT JOIN (
           SELECT bucket, key, COUNT(*) as healthyCopies
           FROM object_copies
           WHERE health_status = 'healthy'
           GROUP BY bucket, key
         ) h ON h.bucket = o.bucket AND h.key = o.key
         WHERE o.deleted = 0
           AND o.status = 'uploaded'
           AND o.piece_cid != ''
           AND COALESCE(h.healthyCopies, 0) < o.desired_copies`
      )
      .get() as { count: number }
    return row.count
  }

  getHealthyObjectCopies(bucket: string, key: string): CopyInfo[] {
    const rows = this.db
      .prepare(`
      SELECT provider_id as providerId, data_set_id as dataSetId, piece_id as pieceId, retrieval_url as retrievalUrl, role
      FROM object_copies
      WHERE bucket = ? AND key = ? AND health_status = 'healthy'
      ORDER BY role ASC
    `)
      .all(bucket, key)
    return rows as CopyInfo[]
  }

  getCopyProbeCandidates(
    limit: number,
    probeIntervalMs: number
  ): Array<{ bucket: string; key: string; providerId: string; retrievalUrl: string }> {
    const seconds = Math.max(1, Math.floor(probeIntervalMs / 1000))
    return this.db
      .prepare(
        `SELECT c.bucket as bucket, c.key as key, c.provider_id as providerId, c.retrieval_url as retrievalUrl
         FROM object_copies c
         JOIN objects o ON o.bucket = c.bucket AND o.key = c.key
         WHERE o.deleted = 0
           AND o.status = 'uploaded'
           AND (c.last_checked_at IS NULL OR c.last_checked_at < datetime('now', ?))
         ORDER BY c.last_checked_at ASC
         LIMIT ?`
      )
      .all(`-${seconds} seconds`, limit) as any
  }

  recordCopyProbeSuccess(bucket: string, key: string, providerId: string): void {
    this.db
      .prepare(
        `UPDATE object_copies
         SET health_status = 'healthy',
             consecutive_failures = 0,
             last_checked_at = datetime('now'),
             last_success_at = datetime('now')
         WHERE bucket = ? AND key = ? AND provider_id = ?`
      )
      .run(bucket, key, providerId)
  }

  recordCopyProbeFailure(bucket: string, key: string, providerId: string, unhealthyFailureThreshold = 24): void {
    const threshold = Math.max(1, Math.floor(unhealthyFailureThreshold))
    this.db
      .prepare(
        `UPDATE object_copies
         SET consecutive_failures = consecutive_failures + 1,
             health_status = CASE
               WHEN consecutive_failures + 1 >= ? THEN 'unhealthy'
               ELSE 'suspect'
             END,
             last_checked_at = datetime('now')
         WHERE bucket = ? AND key = ? AND provider_id = ?`
      )
      .run(threshold, bucket, key, providerId)
  }

  getObjectSummary(): {
    totalFiles: number
    totalBytes: number
    emptyFiles: number
    eligibleFiles: number
  } {
    const row = this.db
      .prepare(
        `SELECT
           COUNT(*) as totalFiles,
           COALESCE(SUM(o.size), 0) as totalBytes,
           SUM(CASE WHEN o.size = 0 THEN 1 ELSE 0 END) as emptyFiles,
           SUM(CASE WHEN o.piece_cid != '' THEN 1 ELSE 0 END) as eligibleFiles
         FROM objects o
         WHERE o.deleted = 0`
      )
      .get() as {
      totalFiles: number
      totalBytes: number
      emptyFiles: number | null
      eligibleFiles: number | null
    }

    return {
      totalFiles: row.totalFiles,
      totalBytes: row.totalBytes,
      emptyFiles: row.emptyFiles ?? 0,
      eligibleFiles: row.eligibleFiles ?? 0,
    }
  }

  getCopyHealthSummary(): {
    eligibleFiles: number
    healthyFiles: number
    suspectFiles: number
    unhealthyFiles: number
    failedFiles: number
  } {
    const row = this.db
      .prepare(
        `SELECT
           COUNT(*) as eligibleFiles,
           SUM(
             CASE
               WHEN healthyCount >= desiredCopies AND suspectCount = 0 AND unhealthyCount = 0 THEN 1
               ELSE 0
             END
           ) as healthyFiles,
           SUM(
             CASE
               WHEN healthyCount >= desiredCopies AND (suspectCount > 0 OR unhealthyCount > 0) THEN 1
               ELSE 0
             END
           ) as suspectFiles,
           SUM(
             CASE
               WHEN healthyCount > 0 AND healthyCount < desiredCopies THEN 1
               ELSE 0
             END
           ) as unhealthyFiles,
           SUM(
             CASE
               WHEN healthyCount = 0 THEN 1
               ELSE 0
             END
           ) as failedFiles
         FROM (
           SELECT
             o.bucket,
             o.key,
             o.desired_copies as desiredCopies,
             COALESCE(SUM(CASE WHEN c.health_status = 'healthy' THEN 1 ELSE 0 END), 0) as healthyCount,
             COALESCE(SUM(CASE WHEN c.health_status = 'suspect' THEN 1 ELSE 0 END), 0) as suspectCount,
             COALESCE(SUM(CASE WHEN c.health_status = 'unhealthy' THEN 1 ELSE 0 END), 0) as unhealthyCount
           FROM objects o
           LEFT JOIN object_copies c ON c.bucket = o.bucket AND c.key = o.key
           WHERE o.deleted = 0
             AND o.piece_cid != ''
           GROUP BY o.bucket, o.key
         )`
      )
      .get() as {
      eligibleFiles: number
      healthyFiles: number | null
      suspectFiles: number | null
      unhealthyFiles: number | null
      failedFiles: number | null
    }

    return {
      eligibleFiles: row.eligibleFiles,
      healthyFiles: row.healthyFiles ?? 0,
      suspectFiles: row.suspectFiles ?? 0,
      unhealthyFiles: row.unhealthyFiles ?? 0,
      failedFiles: row.failedFiles ?? 0,
    }
  }

  updateObjectCopies(bucket: string, key: string, pieceCid: string, copies: CopyInfo[]): void {
    const transaction = this.db.transaction(() => {
      const insertCopy = this.db.prepare(
        `INSERT OR REPLACE INTO object_copies (
           bucket, key, provider_id, data_set_id, piece_id, retrieval_url, role, health_status, consecutive_failures, last_checked_at, last_success_at
         )
         VALUES (?, ?, ?, ?, ?, ?, ?, 'healthy', 0, datetime('now'), datetime('now'))`
      )
      for (const copy of copies) {
        insertCopy.run(bucket, key, copy.providerId, copy.dataSetId, copy.pieceId, copy.retrievalUrl, copy.role)
      }

      const row = this.db
        .prepare('SELECT COUNT(*) as count FROM object_copies WHERE bucket = ? AND key = ?')
        .get(bucket, key) as { count: number }

      this.db
        .prepare(
          `UPDATE objects
           SET piece_cid = ?, copies_count = ?, status = 'uploaded', local_path = NULL, updated_at = datetime('now')
           WHERE bucket = ? AND key = ? AND deleted = 0`
        )
        .run(pieceCid, row.count, bucket, key)
    })

    transaction()
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

  /**
   * Reset stuck 'uploading' objects back to 'pending' on server startup.
   * When the server restarts mid-upload, these objects get stuck because
   * getPendingUploads() only queries 'pending' and 'failed' status.
   * Returns the number of objects reset.
   */
  resetStuckUploads(): number {
    const result = this.db
      .prepare(
        `UPDATE objects SET status = 'pending', updated_at = datetime('now')
         WHERE status = 'uploading' AND deleted = 0`
      )
      .run()
    if (result.changes > 0) {
      this.logger.info({ count: result.changes }, 'reset stuck uploading objects to pending')
    }
    return result.changes
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
