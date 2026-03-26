---
name: add-metadata-store-feature
description: How to add new features to the SQLite metadata store (schema changes, migrations, queries)
---

# Adding Metadata Store Features

## Overview

`src/storage/metadata-store.ts` is the SQLite persistence layer using `better-sqlite3`. It manages:
- **Buckets** and **objects** (S3 key → PieceCID mapping)
- **Object copies** (provider info for download/deletion)
- **Pending deletions** (queue for SP cleanup)
- **Multipart uploads** (temporary state during multi-part uploads)
- **Config** (key-value store for wallet binding, etc.)

## Schema

```sql
-- Core tables
buckets (name TEXT PK, created_at TEXT)
objects (bucket TEXT, key TEXT, piece_cid TEXT, size INT, content_type TEXT,
         etag TEXT, copies_count INT, desired_copies INT, status TEXT,
         local_path TEXT, upload_attempts INT, created_at TEXT,
         updated_at TEXT, deleted INT, PK(bucket, key))
object_copies (bucket TEXT, key TEXT, provider_id TEXT, data_set_id TEXT,
               piece_id TEXT, retrieval_url TEXT, role TEXT,
               health_status TEXT, consecutive_failures INT,
               last_checked_at TEXT, last_success_at TEXT,
               PK(bucket, key, provider_id))
pending_deletions (id INT PK AUTO, piece_cid TEXT, piece_id TEXT,
                   provider_id TEXT, data_set_id TEXT, retrieval_url TEXT,
                   attempts INT, last_attempt TEXT)
multipart_uploads (upload_id TEXT PK, bucket TEXT, key TEXT, content_type TEXT)
multipart_parts (upload_id TEXT, part_number INT, local_path TEXT, size INT, etag TEXT,
                 PK(upload_id, part_number))
config (key TEXT PK, value TEXT)
```

## Adding a New Column

### 1. Add to `initSchema()` CREATE TABLE

Add the column to the CREATE TABLE statement in `initSchema()`. Use a sensible DEFAULT so existing rows remain valid:

```typescript
CREATE TABLE IF NOT EXISTS objects (
  ...existing columns...,
  new_column TEXT NOT NULL DEFAULT ''
);
```

### 2. Add Migration in `migrateSchema()`

For databases that already exist without the new column:

```typescript
private migrateSchema(): void {
  const columns = this.db.prepare("SELECT name FROM pragma_table_info('objects')").all() as Array<{ name: string }>
  const colNames = new Set(columns.map((c) => c.name))

  // ...existing migrations...

  if (!colNames.has('new_column')) {
    this.db.exec("ALTER TABLE objects ADD COLUMN new_column TEXT NOT NULL DEFAULT ''")
    this.logger.info('migrated: added new_column column to objects')
  }
}
```

### 3. Update Query Methods

Update any `INSERT`, `UPDATE`, and `SELECT` statements that need the new column.

**Important**: Column aliases in SELECT must match the TypeScript interface property names:

```typescript
// SQL uses snake_case, TypeScript uses camelCase
const stmt = this.db.prepare(`
  SELECT new_column as newColumn FROM objects WHERE ...
`)
```

## Adding a New Table

### 1. Add CREATE TABLE in `initSchema()`

```typescript
this.db.exec(`
  CREATE TABLE IF NOT EXISTS new_table (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ...columns...,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
  );

  CREATE INDEX IF NOT EXISTS idx_new_table_col ON new_table(some_column);
`)
```

### 2. Add Query Methods

Follow existing patterns:

```typescript
// Insert
addRecord(data: RecordData): void {
  this.db.prepare('INSERT INTO new_table (col1, col2) VALUES (?, ?)').run(data.col1, data.col2)
}

// Select single
getRecord(id: number): RecordData | undefined {
  return this.db.prepare('SELECT col1, col2 FROM new_table WHERE id = ?').get(id) as RecordData | undefined
}

// Select multiple
listRecords(limit = 10): RecordData[] {
  return this.db.prepare('SELECT col1, col2 FROM new_table ORDER BY created_at ASC LIMIT ?').all(limit) as RecordData[]
}

// Update
updateRecord(id: number, col1: string): void {
  this.db.prepare('UPDATE new_table SET col1 = ? WHERE id = ?').run(col1, id)
}

// Delete
removeRecord(id: number): void {
  this.db.prepare('DELETE FROM new_table WHERE id = ?').run(id)
}
```

## Transaction Patterns

Use transactions when multiple statements must be atomic:

```typescript
someAtomicOperation(bucket: string, key: string): void {
  const transaction = this.db.transaction(() => {
    // Step 1: Read existing state
    const existing = this.db.prepare('SELECT ...').get(bucket, key)

    // Step 2: Conditionally update
    if (existing) {
      this.db.prepare('UPDATE ...').run(bucket, key)
    }

    // Step 3: Insert related records
    this.db.prepare('INSERT INTO ...').run(...)
  })

  transaction()  // Execute atomically
}
```

## Testing

Tests live in `src/storage/metadata-store.test.ts`. Use in-memory SQLite:

```typescript
import { MetadataStore } from './metadata-store.js'
import pino from 'pino'

describe('NewFeature', () => {
  let store: MetadataStore

  beforeEach(() => {
    store = new MetadataStore({
      dbPath: ':memory:',
      logger: pino({ level: 'silent' }),
    })
  })

  it('should do something', () => {
    store.createBucket('test-bucket')
    // ...test your new method...
  })
})
```

## Common Pitfalls

1. **WAL mode**: Multiple readers OK, single writer. No concurrent write issues with better-sqlite3 (synchronous API).
2. **Soft delete**: Objects use `deleted = 0/1`. Always include `AND deleted = 0` in queries.
3. **Datetime format**: Use `datetime('now')` for SQLite defaults. Use `toISO8601()` when outputting to S3/WebDAV XML.
4. **noUncheckedIndexedAccess**: Cast query results explicitly: `as SomeType | undefined`.
5. **Migration order**: `migrateSchema()` runs before index creation. If you add an index on a new column, put the `CREATE INDEX` after `migrateSchema()`.
