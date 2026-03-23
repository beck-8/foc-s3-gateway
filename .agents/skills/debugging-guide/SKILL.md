---
name: debugging-guide
description: How to debug common issues in the FOC S3 Gateway
---

# Debugging Guide

## Common Issues and Solutions

### 1. Upload Stuck at "pending"

**Symptom**: Files uploaded but never reach FOC. Status API shows objects stuck as `pending`.

**Check**:
```bash
curl http://localhost:8333/_/status?detail=true
```

**Causes**:
- **UploadWorker not started**: Ensure `uploadWorker.start()` is called in `startServer()`
- **File missing from disk**: Local staged file was deleted before worker picked it up — check `local_path` in SQLite
- **SDK error**: Synapse SDK can't connect to RPC or providers — check logs for `synapse-client` errors
- **Max retries reached**: `upload_attempts >= 10` — the object stays as `failed` permanently

**Fix**: Check `objects` table directly:
```sql
SELECT bucket, key, status, upload_attempts, local_path FROM objects WHERE status != 'uploaded' AND deleted = 0;
```

### 2. Download Returns 404 But Object Exists

**Symptom**: GET returns 404 NoSuchKey even though PutObject succeeded.

**Causes**:
- **Object deleted**: Check `deleted` column — soft-deleted objects return 404
- **Wrong bucket**: Object is in a different bucket
- **URL encoding**: Key contains special characters — ensure proper encoding

### 3. Wallet Address Mismatch on Startup

**Symptom**: Server fails to start with "Wallet address mismatch!" error.

**Cause**: `PRIVATE_KEY` changed since the database was first created.

**Fix**: Either use the original key, or delete the database file and restart:
```bash
# Find the database location
# Linux: ~/.local/share/foc-s3-gateway/metadata.db
# macOS: ~/Library/Application Support/foc-s3-gateway/metadata.db
# Windows: %APPDATA%/foc-s3-gateway/metadata.db
```

### 4. S3 Client Authentication Errors

**Symptom**: 403 AccessDenied from S3 clients.

**Check**:
- Are `--access-key` and `--secret-key` provided to the server?
- Does the S3 client's `access_key_id` match `--access-key`?
- For presigned URLs: Is `X-Amz-Credential` in the query string?
- The gateway does NOT verify AWS signatures — only the access key must match

### 5. Multipart Upload Parts Not Merging

**Symptom**: CompleteMultipartUpload fails or produces wrong output.

**Check**:
- Parts directory: `{dataDir}/multipart/{uploadId}/`
- Part files should be named `part-00001`, `part-00002`, etc.
- All part numbers must be sequential and present
- Check the `multipart_uploads` and `multipart_parts` tables

### 6. CleanupWorker Not Deleting Pieces

**Causes**:
- Worker polls every 10 minutes — wait for next cycle
- SP API errors — check `pending_deletions.attempts` column
- After 5 failed attempts, deletion stops retrying (cooldown: 5 minutes between retries)

## Debugging Tools

### Status API

```bash
# Upload queue summary
curl http://localhost:8333/_/status

# Detailed status with file lists
curl http://localhost:8333/_/status?detail=true
```

### SQLite Direct Access

```bash
# Open the database
sqlite3 ~/.local/share/foc-s3-gateway/metadata.db

# Check overall stats
SELECT status, COUNT(*) FROM objects WHERE deleted = 0 GROUP BY status;

# Check pending uploads
SELECT bucket, key, size, upload_attempts, local_path FROM objects WHERE status IN ('pending','failed') AND deleted = 0;

# Check pending deletions
SELECT * FROM pending_deletions WHERE attempts < 5;

# Check wallet binding
SELECT * FROM config;
```

### Log Levels

Each component has a child logger with `module` field:
- `metadata-store` — database operations
- `synapse-client` — SDK calls (upload/download/delete)
- `local-store` — disk operations
- `upload-worker` — background upload cycle
- `cleanup-worker` — background deletion cycle

Use `LOG_LEVEL=debug` environment variable for verbose output.

### Testing with curl

```bash
# Create bucket
curl -X PUT http://localhost:8333/test-bucket

# Upload file
curl -X PUT http://localhost:8333/test-bucket/hello.txt \
  -H "Content-Type: text/plain" \
  --data-binary "Hello, FOC! This is a test file with enough content to meet the 127 byte minimum size requirement for Filecoin storage providers."

# List objects
curl http://localhost:8333/test-bucket

# Download
curl http://localhost:8333/test-bucket/hello.txt

# Delete
curl -X DELETE http://localhost:8333/test-bucket/hello.txt

# Check upload status
curl http://localhost:8333/_/status?detail=true
```
