/**
 * Tests for S3 XML response builders.
 *
 * These are pure functions that generate XML consumed by S3 clients (Rclone, aws-cli, etc.),
 * so format correctness is critical for compatibility.
 */

import { describe, expect, it } from 'vitest'
import {
  buildCopyObjectResultXml,
  buildDeleteResultXml,
  buildErrorXml,
  buildListBucketsXml,
  buildListObjectsV2Xml,
} from './xml.js'

describe('buildListBucketsXml', () => {
  it('generates valid XML with one bucket', () => {
    const xml = buildListBucketsXml(
      [{ name: 'default', creationDate: '2025-01-01T00:00:00.000Z' }],
      '0xabc123'
    )

    expect(xml).toContain('<?xml version="1.0"')
    expect(xml).toContain('<ListAllMyBucketsResult')
    expect(xml).toContain('<Name>default</Name>')
    expect(xml).toContain('<CreationDate>2025-01-01T00:00:00.000Z</CreationDate>')
    expect(xml).toContain('<ID>0xabc123</ID>')
    expect(xml).toContain('<DisplayName>0xabc123</DisplayName>')
  })

  it('generates valid XML with multiple buckets', () => {
    const xml = buildListBucketsXml(
      [
        { name: 'bucket-a', creationDate: '2025-01-01T00:00:00.000Z' },
        { name: 'bucket-b', creationDate: '2025-06-15T12:00:00.000Z' },
      ],
      'owner'
    )

    expect(xml).toContain('<Name>bucket-a</Name>')
    expect(xml).toContain('<Name>bucket-b</Name>')
  })

  it('generates valid XML with no buckets', () => {
    const xml = buildListBucketsXml([], 'owner')

    expect(xml).toContain('<Buckets>')
    expect(xml).toContain('</Buckets>')
    expect(xml).not.toContain('<Bucket>')
  })

  it('escapes special characters in bucket names', () => {
    const xml = buildListBucketsXml(
      [{ name: 'test<>&"\'bucket', creationDate: '2025-01-01T00:00:00.000Z' }],
      'owner'
    )

    expect(xml).toContain('&lt;')
    expect(xml).toContain('&gt;')
    expect(xml).toContain('&amp;')
    expect(xml).not.toContain('<>')
  })
})

describe('buildListObjectsV2Xml', () => {
  it('generates valid XML with objects', () => {
    const xml = buildListObjectsV2Xml({
      name: 'default',
      prefix: '',
      maxKeys: 1000,
      isTruncated: false,
      contents: [
        {
          key: 'folder/file.txt',
          size: 1024,
          lastModified: '2025-03-20T10:00:00.000Z',
          etag: 'abc123',
          pieceCid: 'baga...',
          contentType: 'text/plain',
        },
      ],
      commonPrefixes: [],
      keyCount: 1,
    })

    expect(xml).toContain('<ListBucketResult')
    expect(xml).toContain('<Name>default</Name>')
    expect(xml).toContain('<Key>folder/file.txt</Key>')
    expect(xml).toContain('<Size>1024</Size>')
    expect(xml).toContain('<ETag>"abc123"</ETag>')
    expect(xml).toContain('<StorageClass>STANDARD</StorageClass>')
    expect(xml).toContain('<IsTruncated>false</IsTruncated>')
    expect(xml).toContain('<KeyCount>1</KeyCount>')
  })

  it('handles empty listing', () => {
    const xml = buildListObjectsV2Xml({
      name: 'default',
      prefix: 'nonexistent/',
      maxKeys: 1000,
      isTruncated: false,
      contents: [],
      commonPrefixes: [],
      keyCount: 0,
    })

    expect(xml).toContain('<KeyCount>0</KeyCount>')
    expect(xml).toContain('<Prefix>nonexistent/</Prefix>')
    expect(xml).not.toContain('<Contents>')
  })

  it('generates common prefixes for delimiter queries', () => {
    const xml = buildListObjectsV2Xml({
      name: 'default',
      prefix: '',
      maxKeys: 1000,
      isTruncated: false,
      contents: [],
      commonPrefixes: ['photos/', 'documents/'],
      keyCount: 0,
    })

    expect(xml).toContain('<CommonPrefixes>')
    expect(xml).toContain('<Prefix>photos/</Prefix>')
    expect(xml).toContain('<Prefix>documents/</Prefix>')
  })

  it('includes NextContinuationToken when truncated', () => {
    const xml = buildListObjectsV2Xml({
      name: 'default',
      prefix: '',
      maxKeys: 2,
      isTruncated: true,
      contents: [
        { key: 'a.txt', size: 10, lastModified: '2025-01-01T00:00:00Z', etag: 'e1', pieceCid: 'p1', contentType: 'text/plain' },
        { key: 'b.txt', size: 20, lastModified: '2025-01-01T00:00:00Z', etag: 'e2', pieceCid: 'p2', contentType: 'text/plain' },
      ],
      commonPrefixes: [],
      keyCount: 2,
      nextContinuationToken: 'b.txt',
    })

    expect(xml).toContain('<IsTruncated>true</IsTruncated>')
    expect(xml).toContain('<NextContinuationToken>b.txt</NextContinuationToken>')
  })

  it('omits NextContinuationToken when not truncated', () => {
    const xml = buildListObjectsV2Xml({
      name: 'default',
      prefix: '',
      maxKeys: 1000,
      isTruncated: false,
      contents: [],
      commonPrefixes: [],
      keyCount: 0,
    })

    expect(xml).not.toContain('NextContinuationToken')
  })
})

describe('buildErrorXml', () => {
  it('generates error XML with resource', () => {
    const xml = buildErrorXml({
      code: 'NoSuchKey',
      message: 'The specified key does not exist.',
      resource: '/default/missing.txt',
      requestId: 'REQ123',
    })

    expect(xml).toContain('<Code>NoSuchKey</Code>')
    expect(xml).toContain('<Message>The specified key does not exist.</Message>')
    expect(xml).toContain('<Resource>/default/missing.txt</Resource>')
    expect(xml).toContain('<RequestId>REQ123</RequestId>')
  })

  it('omits resource when not provided', () => {
    const xml = buildErrorXml({
      code: 'InternalError',
      message: 'Something went wrong',
      requestId: 'REQ456',
    })

    expect(xml).not.toContain('<Resource>')
    expect(xml).toContain('<Code>InternalError</Code>')
  })
})

describe('buildCopyObjectResultXml', () => {
  it('generates correct copy result', () => {
    const xml = buildCopyObjectResultXml('abc123', '2025-03-20T10:00:00.000Z')

    expect(xml).toContain('<CopyObjectResult>')
    expect(xml).toContain('<ETag>"abc123"</ETag>')
    expect(xml).toContain('<LastModified>2025-03-20T10:00:00.000Z</LastModified>')
  })
})

describe('buildDeleteResultXml', () => {
  it('generates delete result with deleted keys', () => {
    const xml = buildDeleteResultXml(['file1.txt', 'file2.txt'], [])

    expect(xml).toContain('<DeleteResult')
    expect(xml).toContain('<Deleted><Key>file1.txt</Key></Deleted>')
    expect(xml).toContain('<Deleted><Key>file2.txt</Key></Deleted>')
  })

  it('generates delete result with errors', () => {
    const xml = buildDeleteResultXml([], [
      { key: 'locked.txt', code: 'AccessDenied', message: 'Access denied' },
    ])

    expect(xml).toContain('<Key>locked.txt</Key>')
    expect(xml).toContain('<Code>AccessDenied</Code>')
    expect(xml).toContain('<Message>Access denied</Message>')
  })

  it('handles mixed deleted and errors', () => {
    const xml = buildDeleteResultXml(
      ['ok.txt'],
      [{ key: 'fail.txt', code: 'InternalError', message: 'Failed' }]
    )

    expect(xml).toContain('<Deleted><Key>ok.txt</Key></Deleted>')
    expect(xml).toContain('<Key>fail.txt</Key>')
  })
})
