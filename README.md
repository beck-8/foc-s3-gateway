# FOC S3 Gateway

S3 + WebDAV gateway for [Filecoin Onchain Cloud (FOC)](https://github.com/FilOzone/synapse-sdk) — use **Rclone**, **AWS CLI**, or any file manager to read/write FOC storage.

> Built for [FOC WG Hackathon #2](https://github.com/FilOzone)

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

## S3 Protocol Compatibility

### ✅ Supported Operations

| Operation | Description |
|-----------|-------------|
| ListBuckets | List all buckets |
| CreateBucket | Create new bucket (`PUT /:bucket`) |
| DeleteBucket | Delete empty bucket (default protected) |
| HeadBucket | Check bucket existence |
| ListObjectsV2 | List with prefix/delimiter/pagination |
| PutObject | Upload to FOC via Synapse SDK |
| GetObject | Download from FOC (primary → secondary → SDK fallback) |
| HeadObject | Get object metadata without downloading |
| DeleteObject | Soft delete in SQLite (data remains on Filecoin) |
| CopyObject | Copy metadata to new key (no re-upload, enables rename/move) |

### ❌ Not Supported

| Operation | Reason |
|-----------|--------|
| Multipart Upload | Not implemented |
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
| Upload latency | Milliseconds | Seconds (on-chain transaction) |
| Delete | Immediate | Soft delete (metadata only; SP data retained) |
| CopyObject | Server-side copy | Metadata-only copy (same PieceCID) |
| Consistency | Strong | Local SQLite = strong; SP = eventual |

## WebDAV Protocol Compatibility

### ✅ Supported Methods

| Method | Description |
|--------|-------------|
| OPTIONS | DAV compliance headers |
| PROPFIND | Directory listing / file properties |
| GET | Download file |
| PUT | Upload file |
| DELETE | Delete file or bucket |
| MKCOL | Create bucket (top-level directory) |
| COPY | Copy file (metadata only) |
| MOVE | Move / rename (copy + delete) |
| HEAD | File metadata |

### ⚠️ Stub Methods (return success but no-op)

| Method | Behavior |
|--------|----------|
| LOCK | Returns fake lock token |
| UNLOCK | Returns 204 |
| PROPPATCH | Returns 200 but doesn't persist custom properties |

### ❌ Not Supported

DAV Class 2/3, Range requests, real locking, custom property persistence

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
├── cli.ts                 # Commander.js CLI
├── server.ts              # S3 Fastify server + startup
├── auth/
│   └── index.ts           # Auth middleware (S3 Sig V4 AK + Basic Auth)
├── routes/
│   └── index.ts           # S3 route handlers
├── s3/
│   ├── types.ts           # S3 type definitions
│   ├── xml.ts             # S3 XML response builders
│   └── errors.ts          # S3 error helpers
├── storage/
│   ├── metadata-store.ts  # SQLite: objects + copies + buckets
│   └── synapse-client.ts  # Synapse SDK wrapper (upload + fallback download)
└── webdav/
    ├── server.ts          # WebDAV Fastify server (separate port)
    ├── routes.ts          # WebDAV method handlers
    └── xml.ts             # DAV XML (multistatus) builders
```

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

### Default Database Paths

| Platform | Path |
|----------|------|
| Linux | `~/.local/share/foc-s3-gateway/metadata.db` |
| macOS | `~/Library/Application Support/foc-s3-gateway/metadata.db` |
| Windows | `%APPDATA%/foc-s3-gateway/metadata.db` |

## How It Works

1. **PutObject** → Data uploaded to FOC via `synapse.storage.upload()`. PieceCID + provider copy info stored in local SQLite
2. **GetObject** → PieceCID looked up in SQLite, downloads via stored provider URLs (primary → secondary fallback → SDK discovery)
3. **ListObjects** → Queried from local SQLite metadata with prefix/delimiter grouping
4. **DeleteObject** → Soft delete in SQLite (data remains on Filecoin SP)
5. **CopyObject** → SQLite metadata copy pointing to same PieceCID (no data movement)

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

Apache-2.0 OR MIT
