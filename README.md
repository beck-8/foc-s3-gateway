# FOC S3 Gateway

S3 + WebDAV gateway for [Filecoin Onchain Cloud (FOC)](https://github.com/FilOzone/synapse-sdk) — use **Rclone**, **AWS CLI**, or any file manager to read/write FOC storage.

# Demo

will be updated later

## What is this?

A Node.js server that provides both **S3** and **WebDAV** interfaces to FOC storage via [Synapse SDK](https://github.com/FilOzone/synapse-sdk):

```
S3 Clients (rclone/aws-cli/mc)  ──S3 API──┐
                                           ├──→ FOC S3 Gateway ──Synapse SDK──→ Filecoin Storage
WebDAV Clients (Finder/Explorer) ──WebDAV──┘
```

**Use cases:**
- 🗄️ Mount FOC to your desktop via Rclone or native file manager
- 📦 `rclone sync` for incremental backup to Filecoin
- 🤖 Any S3 or WebDAV compatible tool
- 💾 Windows/macOS native file system integration via WebDAV

## Quick Start

### 1. Install & Build

```bash
npm install
npm run build
```

### 2. Start the gateway

```bash
# Minimal (no auth, calibration testnet)
export PRIVATE_KEY=0x...
foc-s3-gateway serve

# With authentication
foc-s3-gateway serve \
  --private-key 0x... \
  --access-key myAccessKey \
  --secret-key mySecretKey \
  --network calibration \
  --port 8333 \
  --webdav-port 8334
```

The server starts **two** endpoints:
- **S3** on port `8333` (default)
- **WebDAV** on port `8334` (default: S3 port + 1)

### 3. Connect Clients

#### Rclone (S3)

```ini
[foc]
type = s3
provider = Other
endpoint = http://localhost:8333
access_key_id = myAccessKey
secret_access_key = mySecretKey
```

```bash
rclone ls foc:default
rclone copy ./my-data/ foc:default/backup/
rclone copy foc:default/backup/file.txt ./downloads/
rclone mount foc:default /mnt/foc --vfs-cache-mode full
rclone sync ./important-data foc:default/sync/
rclone moveto foc:default/old.txt foc:default/new.txt    # rename
```

#### Rclone (WebDAV)

```ini
[foc-dav]
type = webdav
url = http://localhost:8334
vendor = other
user = myAccessKey
pass = mySecretKey
```

#### Windows

1. Open **This PC** → Right-click → **Add a network location**
2. Enter `http://localhost:8334` as the address
3. When prompted, enter your Access Key as username and Secret Key as password

> **Note:** Windows WebClient defaults to HTTPS only. For HTTP, set registry key
> `HKLM\SYSTEM\CurrentControlSet\Services\WebClient\Parameters\BasicAuthLevel` to `2`,
> then restart the WebClient service. Alternatively, use **WinSCP** or **Rclone WebDAV** (see above).

#### macOS

Finder → Go → Connect to Server → `http://localhost:8334` (username = Access Key, password = Secret Key)

## Authentication

Authentication is **optional**. If `--access-key` and `--secret-key` are provided, both S3 and WebDAV endpoints require valid credentials.

**The same AK/SK pair is transmitted differently in each protocol:**

| | S3 | WebDAV |
|--|------|--------|
| **Protocol** | AWS Signature V4 | HTTP Basic Auth |
| **AK sent as** | `Authorization: AWS4-HMAC-SHA256 Credential=<AK>/...` | `Authorization: Basic base64(<AK>:<SK>)` |
| **SK usage** | Client uses SK to compute signature (gateway only validates AK match) | Sent directly for comparison |
| **Client config** | `access_key_id` + `secret_access_key` | username + password |

> **Note:** This gateway does NOT verify the AWS Signature V4 signature itself — it only checks that the Access Key matches. This means SK is not actually validated on the S3 side, but S3 clients (rclone, aws-cli, etc.) still require a SK to generate valid requests.

Without credentials, the server runs in open mode (no auth).

## How It Works

### Upload (Async Staging)

Uploads are **asynchronous** — files are staged to local disk first, and the gateway returns success immediately. A background worker then uploads to FOC in the background.

```
Client ──PUT──→ Gateway ──save──→ Local Disk ──200 OK──→ Client
                                      │
                          UploadWorker (background, 10 concurrent)
                                      │
                              Synapse SDK ──→ Filecoin SPs
```

- **Fast response**: PutObject returns in milliseconds (disk I/O only)
- **Concurrent uploads**: Background worker processes up to 10 files in parallel
- **Retry on failure**: Failed uploads retry up to 10 times
- **Immediate availability**: Files can be downloaded immediately after upload (served from local disk)
- **Self-healing replicas**: Background health probes + repair worker automatically refill missing healthy copies

### Download (Local-First)

Downloads prioritize local staged files before going to the network:

1. **Local disk** — if the file is still staged locally (pending/uploading/failed), serve directly from disk
2. **SP direct URLs** — try downloading from known storage provider URLs
3. **SDK discovery** — fall back to Synapse SDK's provider discovery

### Multipart Upload

S3 multipart upload is fully supported for large files:

```
InitiateMultipartUpload → UploadPart (×N) → CompleteMultipartUpload
```

Parts are saved individually to disk, merged into a single file on complete, then queued for async FOC upload.
Each `uploadId` is bound to the original bucket/key pair that created it. Uploading parts, completing, or aborting through a different path is rejected.

## Upload Status API

Monitor the upload queue via the gateway-specific status endpoint:

```bash
# Summary
curl http://localhost:8333/_/status

# Detailed (includes file lists)
curl http://localhost:8333/_/status?detail=true
```

**Example response:**
```json
{
  "objects": { "totalFiles": 120, "totalBytes": 734003200, "totalSize": "700 MB" },
  "replication": {
    "eligibleFiles": 118,
    "compliantFiles": 115,
    "nonCompliantFiles": 3,
    "emptyFiles": 2,
    "repairingFiles": 1,
    "coolingDownFiles": 1
  },
  "repair": {
    "scanIntervalMs": 5000,
    "probeIntervalMs": 3600000,
    "probeTimeoutMs": 8000,
    "unhealthyFailureThreshold": 24,
    "cooldownMs": 300000,
    "pending": 3,
    "probing": 0,
    "inProgress": 1,
    "coolingDown": 1
  },
  "uploads": { "pending": 3, "uploading": 2, "uploaded": 15, "failed": 1 },
  "disk": {
    "staging": { "files": 6, "totalBytes": 52428800, "totalSize": "50.0 MB" },
    "multipartParts": { "files": 0, "totalBytes": 0, "totalSize": "0 B" }
  },
  "multipartUploads": 0
}
```

With `?detail=true`, each pending/uploading/failed object is listed with `bucket`, `key`, `size`, `sizeFormatted`, `uploadAttempts`, and `updatedAt`.

## S3 Protocol Compatibility

### ✅ Supported Operations

| Operation | Description |
|-----------|-------------|
| ListBuckets | List all buckets |
| CreateBucket | Create new bucket (`PUT /:bucket`) |
| DeleteBucket | Delete bucket only when it has no live objects and no active multipart uploads (default protected) |
| HeadBucket | Check bucket existence |
| ListObjectsV2 | List with prefix/delimiter/pagination |
| PutObject | Stage to local disk, async upload to FOC |
| GetObject | Local-first download (disk → SP URLs → SDK) |
| HeadObject | Get object metadata without downloading |
| DeleteObject | Soft delete + cleanup local staged file |
| CopyObject | Copy metadata to new key (no re-upload) |
| InitiateMultipartUpload | Start multipart upload session |
| UploadPart | Upload individual part to disk |
| CompleteMultipartUpload | Merge parts, stage for async FOC upload |
| AbortMultipartUpload | Cancel and cleanup parts |

### ❌ Not Supported

| Operation | Reason |
|-----------|--------|
| DeleteObjects (batch) | Not implemented |
| Versioning | No version management |
| ACL / Bucket Policy | Single-user system |
| ListObjectsV1 | Only V2 supported |
| AWS Signature V4 verification | Only access key is validated, signature not verified |
| Lifecycle / Replication | N/A |

### ⚠️ Behavior Differences vs AWS S3

| Item | AWS S3 | FOC S3 Gateway |
|------|--------|----------------|
| Storage backend | AWS infrastructure | Filecoin SPs (on-chain proofs) |
| **Min file size** | No minimum | **127 bytes** (PieceCID requirement) |
| **Max file size** | 5 TiB (multipart) | **~1 GiB** (1,065,353,216 bytes) |
| Upload latency | Milliseconds | **Milliseconds** (async staging); FOC upload happens in background |
| Storage copies | Region-based | Default 2 (1 primary + 1 secondary SP) |
| Delete | Immediate | Soft delete (metadata only; SP data retained) |
| CopyObject | Server-side copy | Metadata-only copy (same PieceCID) |
| Consistency | Strong | Local SQLite = strong; SP = eventual |

## WebDAV Protocol Compatibility

### ✅ Supported Methods

| Method | Description |
|--------|-------------|
| OPTIONS | DAV compliance headers |
| PROPFIND | Directory listing / file properties |
| GET | Download file (local-first) |
| PUT | Upload file (async staging) |
| DELETE | Delete file or bucket + cleanup local files |
| MKCOL | Create bucket (top-level directory) |
| COPY | Copy file (metadata only, destination bucket must already exist) |
| MOVE | Move / rename (copy + delete, destination bucket must already exist) |
| HEAD | File metadata |

### ⚠️ Stub Methods (return success but no-op)

| Method | Behavior |
|--------|----------|
| LOCK | Returns fake lock token |
| UNLOCK | Returns 204 |
| PROPPATCH | Returns 200 but doesn't persist custom properties |

### ❌ Not Supported

DAV Class 2/3, Range requests (only full-object reads are supported), real locking, custom property persistence

### Path Structure

```
/              → Root (lists buckets as folders)
/default/      → Default bucket contents
/my-bucket/    → Custom bucket contents
/default/a.txt → File
```

## Architecture

```
src/
├── cli.ts                    # Commander.js CLI
├── server.ts                 # S3 Fastify server + startup
├── auth/
│   └── index.ts              # Auth middleware (S3 Sig V4 AK + Basic Auth)
├── routes/
│   └── index.ts              # S3 route handlers (async upload + multipart + status API)
├── s3/
│   ├── types.ts              # S3 type definitions
│   ├── xml.ts                # S3 XML response builders (including multipart)
│   └── errors.ts             # S3 error helpers
├── storage/
│   ├── metadata-store.ts     # SQLite: objects + copies + buckets + multipart + upload status
│   ├── synapse-client.ts     # Synapse SDK wrapper (upload + fallback download)
│   ├── local-store.ts        # Local disk staging (staging/ + multipart/ directories)
│   ├── upload-worker.ts      # Background async FOC upload (10 concurrent, retry up to 10×)
│   └── cleanup-worker.ts     # Background SP piece cleanup for deleted objects
└── webdav/
    ├── server.ts             # WebDAV Fastify server (separate port)
    ├── routes.ts             # WebDAV method handlers (async upload + local-first download)
    └── xml.ts                # DAV XML (multistatus) builders
```

### Data Directory

The gateway stores staged files alongside the database:

```
{dataDir}/
├── metadata.db               # SQLite database
├── staging/                   # Complete files waiting for FOC upload
│   ├── {uuid-1}              # Regular PutObject staged file
│   └── {uuid-2}              # Merged multipart upload
└── multipart/                 # Temporary parts (during active multipart uploads)
    └── {uploadId}/
        ├── part-00001
        ├── part-00002
        └── ...
```

| Platform | Default Data Dir |
|----------|------------------|
| Linux | `~/.local/share/foc-s3-gateway/` |
| macOS | `~/Library/Application Support/foc-s3-gateway/` |
| Windows | `%APPDATA%/foc-s3-gateway/` |

## Configuration

### CLI Options

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

### Repair & Probe Tunables (Env Only)

| Env Var | Default | Description |
|---------|---------|-------------|
| `UPLOAD_SCAN_INTERVAL_MS` | `5000` | Main upload/repair scheduler loop interval |
| `UPLOAD_CONCURRENCY` | `10` | Max concurrent uploads and repair/probe batch size multiplier |
| `COPY_PROBE_INTERVAL_MS` | `3600000` | Minimum re-check interval per copy (1 hour) |
| `COPY_PROBE_TIMEOUT_MS` | `8000` | Timeout per health probe request |
| `COPY_UNHEALTHY_FAILURE_THRESHOLD` | `24` | Consecutive probe failures before marking copy unhealthy |
| `REPAIR_COOLDOWN_MS` | `300000` | Per-object cooldown after failed/incomplete repair (5 minutes) |

Notes:
- A copy in `suspect` / `unhealthy` state does not count as a healthy copy.
- Repair triggers when healthy copy count is below desired copies.
- To emulate "24 hours, 24 consecutive failures", keep `COPY_PROBE_INTERVAL_MS=3600000` and `COPY_UNHEALTHY_FAILURE_THRESHOLD=24`.

## Development

```bash
npm run dev          # Watch mode with tsx
npm run build        # Compile TypeScript
npm run test:unit    # Run vitest
npm run lint:fix     # Biome auto-fix
npm run typecheck    # TypeScript check
npm test             # lint + typecheck + test
```

## License

MIT
