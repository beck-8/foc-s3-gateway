---
name: add-s3-operation
description: How to add a new S3 API operation to the gateway
---

# Adding a New S3 Operation

## Overview

All S3 operations are implemented as Fastify route handlers in `src/routes/index.ts`. They share a common `RouteContext` containing `metadataStore`, `synapseClient`, `localStore`, and `logger`.

## Step-by-Step

### 1. Understand the S3 Operation

Check the [AWS S3 API docs](https://docs.aws.amazon.com/AmazonS3/latest/API/) for:
- HTTP method and URL pattern
- Required/optional headers and query parameters
- Expected XML request/response format
- Status codes and error conditions

### 2. Add Types (if needed)

If the operation involves new request/response types, add them to `src/s3/types.ts`:

```typescript
// src/s3/types.ts
export interface NewOperationResponse {
  // ...fields matching S3 XML output
}
```

### 3. Add XML Builder (if needed)

If the operation returns XML, add a builder function in `src/s3/xml.ts`:

```typescript
// src/s3/xml.ts
export function buildNewOperationXml(data: NewOperationResponse): string {
  return `<?xml version="1.0" encoding="UTF-8"?>
<NewOperationResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  ...
</NewOperationResult>`
}
```

Remember to:
- Use `escapeXml()` for all user-provided values
- Use `toISO8601()` for date fields from SQLite

### 4. Add Route Handler

In `src/routes/index.ts`, inside `registerRoutes()`:

```typescript
// Pattern for bucket-level operations:
// app.get('/:bucket', handler)     — matches GET /bucket (no key)
// app.put('/:bucket', handler)     — matches PUT /bucket (no key)

// Pattern for object-level operations:
// app.get('/:bucket/*', handler)   — matches GET /bucket/key/path
// app.put('/:bucket/*', handler)   — matches PUT /bucket/key/path

// Extract params:
const { bucket } = request.params as { bucket: string }
const key = (request.params as { '*': string })['*']
const query = request.query as Record<string, string>

// Always validate bucket exists:
if (!metadataStore.bucketExists(bucket)) {
  sendNoSuchBucket(reply, bucket)
  return
}
```

**Important**: S3 overloads HTTP methods with query parameters. Distinguish operations by checking query params:

```typescript
// Example: GET /bucket can be ListObjectsV2, GetBucketLocation, or GetBucketVersioning
if ('location' in query) { /* GetBucketLocation */ }
if ('versioning' in query) { /* GetBucketVersioning */ }
// Default: ListObjectsV2
```

### 5. Add Metadata Store Methods (if needed)

If the operation needs new database queries, add methods to `src/storage/metadata-store.ts`:

```typescript
// Use prepared statements for performance
someMethod(bucket: string, key: string): SomeType | undefined {
  const stmt = this.db.prepare(`SELECT ... FROM objects WHERE bucket = ? AND key = ? AND deleted = 0`)
  return stmt.get(bucket, key) as SomeType | undefined
}

// Use transactions for multi-statement operations
someTransaction(bucket: string): void {
  const transaction = this.db.transaction(() => {
    // Multiple statements here run atomically
  })
  transaction()
}
```

### 6. Add WebDAV Equivalent (if applicable)

If the S3 operation has a WebDAV counterpart, implement it in `src/webdav/routes.ts`:
- Use `parseDavPath(request.url)` to extract `{ bucket, key }`
- Share the same storage layer (`metadataStore`, `localStore`, `synapseClient`)
- Use WebDAV-appropriate status codes (207 for multistatus, 201 for created, etc.)

### 7. Add Tests

Add tests in the appropriate test file:
- XML builders: `src/s3/xml.test.ts`
- Metadata operations: `src/storage/metadata-store.test.ts`
- WebDAV routes: `src/webdav/routes.test.ts`

```typescript
// src/s3/xml.test.ts
describe('buildNewOperationXml', () => {
  it('should produce valid XML', () => {
    const xml = buildNewOperationXml({ /* test data */ })
    expect(xml).toContain('<NewOperationResult')
    // assert specific fields
  })
})
```

### 8. Run Checks

```bash
npm run lint:fix && npm run typecheck && npm run test:unit
```

## Error Handling Patterns

```typescript
import { sendS3Error, sendNoSuchBucket, sendNoSuchKey, sendInternalError } from '../s3/errors.js'

// Common errors:
sendNoSuchBucket(reply, bucket)        // 404
sendNoSuchKey(reply, key)              // 404
sendInternalError(reply, 'message')    // 500
sendS3Error(reply, 409, 'BucketAlreadyOwnedByYou', 'message', bucket)  // custom
```

## Upload Flow Notes

For operations that involve uploads:
1. Validate `Content-Length` header first (fast reject)
2. Stream to local disk via `localStore.stageUpload(id, request.raw)`
3. Validate actual size after streaming
4. Call `metadataStore.stageObject()` to queue for async upload
5. Return success immediately (200/201)
6. The `UploadWorker` will handle the FOC upload in background

## Common Pitfalls

1. **Import extensions**: Always use `.js` extensions (`from './foo.js'`)
2. **Trailing slash**: `GET /bucket/` has empty key `''` — handle as bucket-level
3. **Semicolons**: Must not use semicolons (Biome `asNeeded`)
4. **Query params**: Use bracket notation `query['param']` due to `noUncheckedIndexedAccess`
5. **S3 idempotency**: DELETE always returns 204, even for nonexistent objects
