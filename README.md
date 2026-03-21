# FOC S3 Gateway

S3-compatible gateway for [Filecoin Onchain Cloud (FOC)](https://github.com/FilOzone/synapse-sdk) — use **Rclone** or any S3 client to read/write FOC storage.

> Built for [FOC WG Hackathon #2](https://github.com/FilOzone)

## What is this?

A lightweight Node.js server that speaks S3 protocol and maps operations to FOC storage via [Synapse SDK](https://github.com/FilOzone/synapse-sdk):

```
Rclone / AWS CLI / S3 Client ──S3 API──> FOC S3 Gateway ──Synapse SDK──> Filecoin Storage
```

**Use cases:**
- 🗄️ Mount FOC to your NAS via Rclone
- 📦 `rclone sync` for incremental backup to Filecoin
- 🤖 Any S3-compatible tool (CyberDuck, aws-cli, etc.)

## Quick Start

### 1. Install & Build

```bash
npm install
npm run build
```

### 2. Start the gateway

```bash
# Using environment variable
export PRIVATE_KEY=0x...
foc-s3-gateway serve

# Or with flags
foc-s3-gateway serve --private-key 0x... --network calibration --port 8333
```

### 3. Configure Rclone

```ini
[foc]
type = s3
provider = Other
endpoint = http://localhost:8333
access_key_id = any
secret_access_key = any
```

### 4. Use it!

```bash
# List files
rclone ls foc:default

# Upload
rclone copy ./my-data/ foc:default/backup/

# Download
rclone copy foc:default/backup/file.txt ./downloads/

# Mount as local drive (NAS scenario!)
rclone mount foc:default /mnt/foc --vfs-cache-mode full

# Sync
rclone sync ./important-data foc:default/sync/
```

## Architecture

```
src/
├── cli.ts              # Commander.js CLI entry point
├── server.ts           # Fastify server setup
├── index.ts            # Library exports
├── routes/
│   └── index.ts        # S3 route handlers (ListBuckets, Get/Put/Delete Object, etc.)
├── s3/
│   ├── types.ts        # S3 type definitions
│   ├── xml.ts          # S3 XML response builders
│   ├── errors.ts       # S3 error response helpers
│   └── index.ts        # Re-exports
└── storage/
    ├── metadata-store.ts   # SQLite key→PieceCID mapping
    ├── synapse-client.ts   # Synapse SDK wrapper
    └── index.ts            # Re-exports
```

## Supported S3 Operations

| Operation | Status | Description |
|-----------|--------|-------------|
| ListBuckets | ✅ | Returns default bucket |
| HeadBucket | ✅ | Validates bucket existence |
| ListObjectsV2 | ✅ | List with prefix/delimiter support |
| PutObject | ✅ | Upload to FOC via Synapse SDK |
| GetObject | ✅ | Download from FOC |
| HeadObject | ✅ | Get object metadata |
| DeleteObject | ✅ | Soft delete (metadata only) |

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `PRIVATE_KEY` | Yes* | — | Wallet private key (with USDFC tokens) |
| `RPC_URL` | No | Calibration | Filecoin RPC endpoint |
| `PORT` | No | `8333` | Server port |
| `HOST` | No | `0.0.0.0` | Server host |

\* Can also be passed via `--private-key` flag

## Development

```bash
npm run dev          # Watch mode with tsx
npm run build        # Compile TypeScript
npm run lint         # Biome check
npm run lint:fix     # Biome auto-fix
npm run typecheck    # TypeScript check
```

## How It Works

1. **PutObject** → Body is uploaded to FOC via `synapse.storage.upload()`, PieceCID is stored in local SQLite
2. **GetObject** → PieceCID looked up in SQLite, data downloaded via `synapse.storage.download()`
3. **ListObjects** → Queried from local SQLite metadata
4. **DeleteObject** → Soft delete in SQLite (data remains on Filecoin)

## License

Apache-2.0 OR MIT
