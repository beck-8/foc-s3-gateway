---
name: add-webdav-method
description: How to add a new WebDAV method to the gateway
---

# Adding a New WebDAV Method

## Overview

WebDAV routes run on a separate Fastify instance (default port 8334). They live in `src/webdav/routes.ts` and share the same storage layer as S3 routes.

## Architecture

```
src/webdav/
├── server.ts     # Fastify server setup (separate from S3)
├── routes.ts     # All WebDAV method handlers
├── routes.test.ts # Tests
└── xml.ts        # DAV XML builders (multistatus)
```

## Step-by-Step

### 1. Register the Route

WebDAV uses non-standard HTTP methods. Register with `app.route()`:

```typescript
// In registerWebDavRoutes() in src/webdav/routes.ts
app.route({
  method: 'NEWMETHOD',        // Custom HTTP method
  url: '/*',                  // Catch-all for paths
  handler: async (request: FastifyRequest, reply: FastifyReply) => {
    const { bucket, key } = parseDavPath(request.url)
    logger.debug({ bucket, key }, 'WebDAV NEWMETHOD')

    // Implementation...
  },
})
```

### 2. Path Parsing

Use the shared `parseDavPath()` helper:

```typescript
function parseDavPath(url: string): { bucket?: string; key?: string }

// Examples:
// "/"              → {}                           (root)
// "/my-bucket/"    → { bucket: 'my-bucket' }     (bucket level)
// "/bucket/a/b.txt"→ { bucket: 'bucket', key: 'a/b.txt' }  (file level)
```

### 3. Common Response Patterns

```typescript
// Success
reply.status(200).send('OK')
reply.status(201).send()           // Created
reply.status(204).send()           // No Content (delete)
reply.status(207)                  // Multistatus (PROPFIND)
  .header('Content-Type', 'application/xml; charset=utf-8')
  .send(xmlResponse)

// Errors
reply.status(400).send('Bad Request')
reply.status(403).send('Forbidden')
reply.status(404).send('Not Found')
reply.status(409).send('Conflict')
reply.status(500).send('Internal Server Error')
```

### 4. WebDAV-Specific Headers

DAV methods often use special headers:

```typescript
// Depth header (PROPFIND)
const depth = getDepth(request)  // 0, 1, or Infinity

// Destination header (COPY, MOVE)
const destination = request.headers['destination'] as string | undefined
const destUrl = new URL(destination, `http://${request.headers.host}`)
const { bucket: dstBucket, key: dstKey } = parseDavPath(destUrl.pathname)

// Lock-Token header (LOCK)
reply.header('Lock-Token', `<opaquelocktoken:${randomToken}>`)
```

### 5. Multistatus XML

For PROPFIND responses, use the `DavResource` type and `buildMultistatusXml()`:

```typescript
import type { DavResource } from './xml.js'
import { buildMultistatusXml } from './xml.js'

const resources: DavResource[] = [
  {
    href: '/bucket/file.txt',
    displayName: 'file.txt',
    isCollection: false,
    contentLength: 1024,
    contentType: 'text/plain',
    lastModified: '2026-03-21 13:57:52',
    etag: 'abc123',
  },
]

const xml = buildMultistatusXml(resources)
reply.status(207).header('Content-Type', 'application/xml; charset=utf-8').send(xml)
```

### 6. Authentication

WebDAV auth is handled by the same auth middleware as S3. It detects Basic Auth:

```
Authorization: Basic base64(accessKey:secretKey)
```

No special handling needed in route code — the `preHandler` hook handles it.

### 7. Stub Methods

For methods that clients expect but you don't want to fully implement:

```typescript
// Stub that returns success without doing anything
app.route({
  method: 'NEWMETHOD',
  url: '/*',
  handler: async (_request: FastifyRequest, reply: FastifyReply) => {
    reply.status(200).send()  // or 204
  },
})
```

Existing stubs: LOCK (returns fake token), UNLOCK (204), PROPPATCH (returns 207 multistatus).

### 8. Add Tests

In `src/webdav/routes.test.ts`:

```typescript
describe('WebDAV NEWMETHOD', () => {
  it('should handle file-level requests', () => {
    // Test path parsing and expected behavior
  })
})
```

## Upload/Download in WebDAV

WebDAV PUT/GET use the same async staging flow as S3:

```typescript
// PUT → stage to disk → return 201 → UploadWorker handles FOC upload
const stageId = randomUUID()
const staged = await localStore.stageUpload(stageId, request.raw)
metadataStore.stageObject(bucket, key, staged.size, contentType, staged.etag, staged.localPath)
reply.status(201).send()

// GET → local disk first → SP direct URLs → SDK discovery
const localPath = metadataStore.getLocalPath(bucket, key)
if (localPath && localStore.exists(localPath)) {
  // Serve from local disk
} else {
  // Fall back to FOC download
}
```

## Size Constraints

Same as S3:
- **Minimum**: 127 bytes
- **Maximum**: 1,065,353,216 bytes (~1 GiB)
- **Empty objects**: Allowed (stored as metadata only)
