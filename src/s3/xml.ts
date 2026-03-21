/** S3 XML response builders */

import type { ListObjectsV2Response, S3Bucket, S3ErrorResponse } from './types.js'

function escapeXml(str: string): string {
  return str
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&apos;')
}

/** Convert SQLite datetime ("YYYY-MM-DD HH:mm:ss") or ISO string to S3-compatible ISO 8601 */
function toISO8601(dateStr: string): string {
  // Already ISO 8601 (contains "T")
  if (dateStr.includes('T')) return dateStr
  // SQLite format: "2026-03-21 13:57:52" → "2026-03-21T13:57:52.000Z"
  return new Date(`${dateStr}Z`).toISOString()
}

export function buildListBucketsXml(buckets: S3Bucket[], ownerId: string): string {
  const bucketEntries = buckets
    .map(
      (b) => `    <Bucket>
      <Name>${escapeXml(b.name)}</Name>
      <CreationDate>${escapeXml(toISO8601(b.creationDate))}</CreationDate>
    </Bucket>`
    )
    .join('\n')

  return `<?xml version="1.0" encoding="UTF-8"?>
<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Owner>
    <ID>${escapeXml(ownerId)}</ID>
    <DisplayName>${escapeXml(ownerId)}</DisplayName>
  </Owner>
  <Buckets>
${bucketEntries}
  </Buckets>
</ListAllMyBucketsResult>`
}

export function buildListObjectsV2Xml(response: ListObjectsV2Response): string {
  const contents = response.contents
    .map(
      (obj) => `  <Contents>
    <Key>${escapeXml(obj.key)}</Key>
    <LastModified>${escapeXml(toISO8601(obj.lastModified))}</LastModified>
    <ETag>"${escapeXml(obj.etag)}"</ETag>
    <Size>${obj.size}</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>`
    )
    .join('\n')

  const prefixes = response.commonPrefixes
    .map(
      (p) => `  <CommonPrefixes>
    <Prefix>${escapeXml(p)}</Prefix>
  </CommonPrefixes>`
    )
    .join('\n')

  const continuationToken = response.nextContinuationToken
    ? `  <NextContinuationToken>${escapeXml(response.nextContinuationToken)}</NextContinuationToken>`
    : ''

  return `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>${escapeXml(response.name)}</Name>
  <Prefix>${escapeXml(response.prefix)}</Prefix>
  <MaxKeys>${response.maxKeys}</MaxKeys>
  <IsTruncated>${response.isTruncated}</IsTruncated>
  <KeyCount>${response.keyCount}</KeyCount>
${continuationToken}
${contents}
${prefixes}
</ListBucketResult>`
}

export function buildErrorXml(error: S3ErrorResponse): string {
  const resource = error.resource ? `  <Resource>${escapeXml(error.resource)}</Resource>` : ''

  return `<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>${escapeXml(error.code)}</Code>
  <Message>${escapeXml(error.message)}</Message>
${resource}
  <RequestId>${escapeXml(error.requestId)}</RequestId>
</Error>`
}

export function buildCopyObjectResultXml(etag: string, lastModified: string): string {
  return `<?xml version="1.0" encoding="UTF-8"?>
<CopyObjectResult>
  <ETag>"${escapeXml(etag)}"</ETag>
  <LastModified>${escapeXml(toISO8601(lastModified))}</LastModified>
</CopyObjectResult>`
}

export function buildDeleteResultXml(
  deleted: string[],
  errors: Array<{ key: string; code: string; message: string }>
): string {
  const deletedEntries = deleted.map((key) => `  <Deleted><Key>${escapeXml(key)}</Key></Deleted>`).join('\n')

  const errorEntries = errors
    .map(
      (e) => `  <Error>
    <Key>${escapeXml(e.key)}</Key>
    <Code>${escapeXml(e.code)}</Code>
    <Message>${escapeXml(e.message)}</Message>
  </Error>`
    )
    .join('\n')

  return `<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
${deletedEntries}
${errorEntries}
</DeleteResult>`
}
