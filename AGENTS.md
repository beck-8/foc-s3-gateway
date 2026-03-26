# FOC S3 Gateway

S3 + WebDAV gateway for Filecoin Onchain Cloud (FOC). Uses Rclone/AWS CLI/native file managers to read/write FOC storage via Synapse SDK.

## What This Is

Node.js server (Fastify) providing dual S3 + WebDAV interfaces to FOC storage. Uploads are async-staged to local disk, background workers upload to Filecoin SPs via Synapse SDK.

**Stack**: TypeScript, Fastify, SQLite (better-sqlite3), Synapse SDK (`@filoz/synapse-sdk` + `@filoz/synapse-core`), Commander.js, Pino logging, Vitest, Biome.

**Entry points**: `src/cli.ts` (CLI), `src/server.ts` (server factory), `src/index.ts` (library exports).

## Architecture

```
src/
├── cli.ts                    # Commander.js CLI (serve command)
├── server.ts                 # Fastify server factory + startup (S3 + WebDAV dual port)
├── index.ts                  # Library exports (createServer, startServer)
├── auth/
│   ├── index.ts              # Auth middleware (S3 Sig V4 AK extraction + Basic Auth)
│   └── index.test.ts         # Auth middleware tests
├── routes/
│   ├── index.ts              # S3 route handlers (CRUD + multipart + status API)
│   └── index.test.ts         # S3 route integration tests
├── s3/
│   ├── types.ts              # S3 type definitions (S3Object, S3Bucket, etc.)
│   ├── xml.ts                # S3 XML response builders
│   ├── xml.test.ts           # XML builder tests
│   ├── errors.ts             # S3 error helpers
│   ├── range.ts              # HTTP Range header parser (RFC 7233)
│   ├── range.test.ts         # Range parser tests
│   └── index.ts              # Re-exports
├── storage/
│   ├── metadata-store.ts     # SQLite: objects + copies + buckets + multipart + config
│   ├── metadata-store.test.ts# MetadataStore unit tests
│   ├── synapse-client.ts     # Synapse SDK wrapper (upload + fallback download + delete)
│   ├── local-store.ts        # Local disk staging (staging/ + multipart/ directories)
│   ├── upload-worker.ts      # Background async FOC upload (10 concurrent, retry ×10)
│   ├── probe-worker.ts       # Copy health probe (HEAD/Range checks per SP)
│   ├── repair-worker.ts      # Under-replicated repair (SP-to-SP pull + cooldown)
│   ├── cleanup-worker.ts     # Background SP piece cleanup for deleted objects
│   └── index.ts              # Re-exports
└── webdav/
    ├── server.ts             # WebDAV Fastify server (separate port)
    ├── routes.ts             # WebDAV method handlers (async upload + local-first + range)
    ├── routes.test.ts        # WebDAV route tests
    └── xml.ts                # DAV XML (multistatus) builders
```

### Data Flow

```
Upload:   Client ──PUT──→ Gateway ──save──→ Local Disk ──200──→ Client
                                                │
                                    UploadWorker (background, 10 concurrent)
                                                │
                                        Synapse SDK ──→ Filecoin SPs

Download: Client ──GET──→ Gateway ──→ Local disk? → SP direct URLs? → SDK discovery
```

### Key Design Decisions

1. **Async staging**: PutObject returns immediately after disk write. Background UploadWorker handles FOC upload.
2. **Local-first download**: Staged files served from local disk before going to network.
3. **Dual protocol**: S3 (port 8333) + WebDAV (port 8334) share same storage layer.
4. **Soft delete**: deleteObject marks `deleted=1` in SQLite, queues SP piece cleanup via CleanupWorker.
5. **Wallet binding**: First startup binds wallet address to database. Prevents accidental key changes.
6. **Self-healing replicas**: ProbeWorker checks copy health via HEAD requests; RepairWorker re-uploads missing copies.
7. **Range requests**: S3 and WebDAV both support single byte-range requests (RFC 7233, 206 Partial Content).

## Development

```bash
npm install && npm run build     # Setup
npm run dev                      # Watch mode with tsx
npm test                         # lint + typecheck + vitest
npm run test:unit                # vitest only
npm run lint:fix                 # Biome auto-fix
npm run typecheck                # tsc --noEmit
```

## Biome Linting (Critical)

- **NO** semicolons at line end (`semicolons: "asNeeded"`)
- **MUST** use `.js` extensions in imports (`import {x} from './y.js'` even for .ts files)
- **MUST** use kebab-case filenames
- **MUST** use single quotes
- Line width: 120, indent: 2 spaces
- Trailing commas: `es5`

## TypeScript Config

- `strict: true`, `noUnusedLocals`, `noUnusedParameters`, `exactOptionalPropertyTypes`
- `noUncheckedIndexedAccess: true` — array/object index access returns `T | undefined`
- Target: ES2022, Module: NodeNext
- Tests excluded from compilation (`src/**/*.test.ts`)

## Key Patterns

### SQLite Metadata Store

- WAL mode, synchronous=NORMAL for performance
- Schema auto-migration for new columns (`migrateSchema()`)
- Transactions via `db.transaction()` for multi-statement consistency
- Tables: `buckets`, `objects`, `object_copies`, `pending_deletions`, `multipart_uploads`, `multipart_parts`, `config`
- Upload status lifecycle: `pending` → `uploading` → `uploaded` (or `failed`)

### Upload Worker

- Polls every 5s for pending/failed objects (< 10 attempts)
- Up to 10 concurrent uploads via `Promise.allSettled()`
- Creates web `ReadableStream` from local file for Synapse SDK
- On success: updates metadata with pieceCid/copies + deletes local file
- On failure: marks `failed`, preserves local file for retry

### Probe Worker

- Polls every 5s for copies needing health check (oldest `last_checked_at` first)
- Sends HEAD or Range probe to each SP retrieval URL
- On success: resets `consecutive_failures`, marks `healthy`
- On failure: increments `consecutive_failures`; marks `suspect` then `unhealthy` at threshold (default 24)

### Repair Worker

- Polls every 5s for under-replicated objects (healthy copies < desired_copies)
- Re-uploads data to new SPs via Synapse SDK
- Per-object cooldown (default 5 min) prevents retry storms
- Up to 10 concurrent repairs via `Promise.allSettled()`

### Cleanup Worker

- Polls every 10 minutes for pending piece deletions
- Up to 5 attempts per deletion with 5-minute cooldown
- Uses `schedulePieceDeletion()` from `@filoz/synapse-core/sp`
- Treats "not found" errors as success (piece already gone)

### Authentication

- **Optional**: only enforced if `--access-key` and `--secret-key` are provided
- **S3**: Extracts AK from AWS Sig V4 `Authorization` header (does NOT verify signature)
- **WebDAV**: Standard HTTP Basic Auth (username=AK, password=SK)
- **Presigned URLs**: Checks `X-Amz-Credential` query parameter
- **Internal endpoints** (`/_/*`): Always skip auth

### S3 Constraints

- **Min file size**: 127 bytes (PieceCID requirement)
- **Max file size**: ~1 GiB (1,065,353,216 bytes)
- **Empty objects**: Allowed (stored as metadata only, no FOC upload)
- **CopyObject**: Metadata-only copy (same PieceCID, no re-upload)
- **DELETE**: Idempotent, always returns 204

### WebDAV Specifics

- Path parsing: `/{bucket}/{key}` structure
- LOCK/UNLOCK/PROPPATCH: Stub implementations (return success but no-op)
- MKCOL: Creates bucket (top-level directory only)
- MOVE: Implemented as copy + delete (preserves local_path ownership)
- Range requests: Supported (same as S3, 206 Partial Content)
- DeleteObjects: Batch delete via `POST /{bucket}?delete`

## Testing

- **Framework**: Vitest with `globals: true`
- **Test files**: `src/**/*.test.ts` (co-located with source)
- **Existing tests**: `s3/xml.test.ts`, `storage/metadata-store.test.ts`, `webdav/routes.test.ts`
- In-memory SQLite (`:memory:`) for MetadataStore tests

## Configuration

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `-k, --private-key` | `PRIVATE_KEY` | — | **Required.** Wallet private key |
| `-p, --port` | `PORT` | `8333` | S3 server port |
| `-H, --host` | `HOST` | `0.0.0.0` | Bind address |
| `-n, --network` | `NETWORK` | `calibration` | `mainnet` or `calibration` |
| `-r, --rpc-url` | `RPC_URL` | auto | Filecoin RPC endpoint |
| `-d, --db-path` | `DB_PATH` | platform-specific | SQLite database path |
| `-a, --access-key` | `ACCESS_KEY` | — | Authentication access key |
| `-s, --secret-key` | `SECRET_KEY` | — | Authentication secret key |
| `-w, --webdav-port` | — | S3 port + 1 | WebDAV server port |
| `-c, --copies` | `COPIES` | `2` | Desired copies for newly uploaded objects |

### Worker Tunables (Env Only)

| Env Var | Default | Description |
|---------|---------|-------------|
| `UPLOAD_SCAN_INTERVAL_MS` | `5000` | Upload worker scan interval |
| `UPLOAD_CONCURRENCY` | `10` | Max concurrent uploads |
| `PROBE_SCAN_INTERVAL_MS` | `5000` | Probe worker scan interval |
| `PROBE_CONCURRENCY` | `10` | Max concurrent probe requests |
| `COPY_PROBE_INTERVAL_MS` | `3600000` | Minimum re-check interval per copy (1 hour) |
| `COPY_PROBE_TIMEOUT_MS` | `5000` | Timeout per health probe request |
| `COPY_UNHEALTHY_FAILURE_THRESHOLD` | `24` | Consecutive probe failures → unhealthy |
| `REPAIR_SCAN_INTERVAL_MS` | `5000` | Repair worker scan interval |
| `REPAIR_CONCURRENCY` | `10` | Max concurrent object repairs |
| `REPAIR_COOLDOWN_MS` | `300000` | Per-object cooldown after failed repair (5 min) |

## Data Directory

| Platform | Default |
|----------|---------|
| Linux | `~/.local/share/foc-s3-gateway/` |
| macOS | `~/Library/Application Support/foc-s3-gateway/` |
| Windows | `%APPDATA%/foc-s3-gateway/` |

## Git Policy

Conventional commits. Never `git commit` or `git push` without explicit user permission.
