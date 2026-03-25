/**
 * Tests for SQLite metadata store.
 *
 * Uses in-memory SQLite (:memory:) for fast, isolated tests.
 * This is the core mapping layer — if it's wrong, S3 operations return incorrect data.
 */

import pino from 'pino'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { MetadataStore } from './metadata-store.js'

const logger = pino({ level: 'silent' })

describe('MetadataStore', () => {
  let store: MetadataStore

  beforeEach(() => {
    store = new MetadataStore({ dbPath: ':memory:', logger })
  })

  afterEach(() => {
    store.close()
  })

  // ── Bucket operations ────────────────────────────────────────────────

  describe('createBucket / bucketExists / listBuckets', () => {
    it('default bucket exists on init', () => {
      expect(store.bucketExists('default')).toBe(true)
    })

    it('creates and finds a new bucket', () => {
      const created = store.createBucket('photos')

      expect(created).toBe(true)
      expect(store.bucketExists('photos')).toBe(true)
    })

    it('returns false for duplicate bucket creation', () => {
      store.createBucket('photos')
      const duplicate = store.createBucket('photos')

      expect(duplicate).toBe(false)
    })

    it('lists all buckets including default', () => {
      store.createBucket('videos')
      store.createBucket('audio')

      const buckets = store.listBuckets()

      expect(buckets.length).toBeGreaterThanOrEqual(3)
      const names = buckets.map((b) => b.name)
      expect(names).toContain('default')
      expect(names).toContain('videos')
      expect(names).toContain('audio')
    })

    it('returns false for non-existent bucket', () => {
      expect(store.bucketExists('nonexistent')).toBe(false)
    })
  })

  describe('deleteBucket', () => {
    it('deletes an empty bucket', () => {
      store.createBucket('temp')
      const deleted = store.deleteBucket('temp')

      expect(deleted).toBe(true)
      expect(store.bucketExists('temp')).toBe(false)
    })

    it('refuses to delete default bucket', () => {
      const deleted = store.deleteBucket('default')

      expect(deleted).toBe(false)
      expect(store.bucketExists('default')).toBe(true)
    })

    it('refuses to delete bucket with objects', () => {
      store.createBucket('data')
      store.putObject('data', 'file.txt', 'cid', 100, 'text/plain', 'etag')

      const deleted = store.deleteBucket('data')

      expect(deleted).toBe(false)
      expect(store.bucketExists('data')).toBe(true)
    })

    it('can delete bucket after all objects are soft-deleted', () => {
      store.createBucket('data')
      store.putObject('data', 'file.txt', 'cid', 100, 'text/plain', 'etag')
      store.deleteObject('data', 'file.txt')

      const deleted = store.deleteBucket('data')

      expect(deleted).toBe(true)
    })

    it('returns false for non-existent bucket', () => {
      const deleted = store.deleteBucket('nonexistent')
      expect(deleted).toBe(false)
    })

    it('refuses to delete bucket with active multipart uploads', () => {
      store.createBucket('uploads')
      store.createMultipartUpload('upload-1', 'uploads', 'movie.bin', 'application/octet-stream')

      const deleted = store.deleteBucket('uploads')

      expect(deleted).toBe(false)
      expect(store.bucketExists('uploads')).toBe(true)
    })
  })

  describe('desired copies snapshot', () => {
    it('uses default desired copies when staging', () => {
      store.stageObject('default', 'a.bin', 256, 'application/octet-stream', 'etag-a', '/tmp/a')

      const pending = store.getPendingUploads(10)
      expect(pending).toHaveLength(1)
      expect(pending[0]?.desiredCopies).toBe(2)
    })

    it('applies new default only to newly staged objects', () => {
      store.stageObject('default', 'old.bin', 256, 'application/octet-stream', 'etag-old', '/tmp/old')
      store.setDefaultDesiredCopies(3)
      store.stageObject('default', 'new.bin', 256, 'application/octet-stream', 'etag-new', '/tmp/new')

      const pending = store.getPendingUploads(10)
      const oldObj = pending.find((p) => p.key === 'old.bin')
      const newObj = pending.find((p) => p.key === 'new.bin')

      expect(oldObj?.desiredCopies).toBe(2)
      expect(newObj?.desiredCopies).toBe(3)
    })

    it('marks partial upload as uploaded and eligible for repair', () => {
      store.stageObject('default', 'repair.bin', 256, 'application/octet-stream', 'etag-repair', '/tmp/repair')
      store.recordPartialUpload('default', 'repair.bin', 'cid-repair', [
        {
          providerId: '42',
          dataSetId: '100',
          pieceId: '1',
          retrievalUrl: 'https://sp1.example.com/piece/cid-repair',
          role: 'primary',
        },
      ])

      const obj = store.getObject('default', 'repair.bin')
      expect(obj?.pieceCid).toBe('cid-repair')

      const candidates = store.getUnderReplicatedObjects(10)
      const repairItem = candidates.find((c) => c.key === 'repair.bin')
      expect(repairItem?.desiredCopies).toBe(2)
      expect(repairItem?.copiesCount).toBe(1)
      expect(repairItem?.healthyCopies).toBe(1)
    })
  })

  // ── Object copies ───────────────────────────────────────────────────

  describe('putObject with copies / getObjectCopies', () => {
    const testCopies = [
      {
        providerId: '42',
        dataSetId: '100',
        pieceId: '1000',
        retrievalUrl: 'https://sp1.example.com/piece/baga1',
        role: 'primary' as const,
      },
      {
        providerId: '99',
        dataSetId: '200',
        pieceId: '1000',
        retrievalUrl: 'https://sp2.example.com/piece/baga1',
        role: 'secondary' as const,
      },
    ]

    it('stores and retrieves copies', () => {
      store.putObject('default', 'doc.pdf', 'baga1', 1024, 'application/pdf', 'etag1', testCopies)

      const copies = store.getObjectCopies('default', 'doc.pdf')

      expect(copies).toHaveLength(2)
      const primary = copies.find((c) => c.role === 'primary')
      const secondary = copies.find((c) => c.role === 'secondary')
      expect(primary?.providerId).toBe('42')
      expect(primary?.retrievalUrl).toBe('https://sp1.example.com/piece/baga1')
      expect(secondary?.providerId).toBe('99')
    })

    it('returns empty array for object without copies', () => {
      store.putObject('default', 'no-copies.txt', 'cid', 10, 'text/plain', 'etag')

      const copies = store.getObjectCopies('default', 'no-copies.txt')

      expect(copies).toHaveLength(0)
    })

    it('replaces copies on re-upload', () => {
      store.putObject('default', 'evolving.txt', 'cid-v1', 100, 'text/plain', 'e1', testCopies)

      const newCopies = [
        {
          providerId: '77',
          dataSetId: '300',
          pieceId: '3000',
          retrievalUrl: 'https://sp3.example.com/piece/cid-v2',
          role: 'primary' as const,
        },
      ]
      store.putObject('default', 'evolving.txt', 'cid-v2', 200, 'text/plain', 'e2', newCopies)

      const copies = store.getObjectCopies('default', 'evolving.txt')

      expect(copies).toHaveLength(1)
      expect(copies[0]?.providerId).toBe('77')
    })

    it('works with object params style', () => {
      store.putObject({
        bucket: 'default',
        key: 'objstyle.txt',
        pieceCid: 'baga-obj',
        size: 512,
        contentType: 'text/plain',
        etag: 'etag-obj',
        copies: testCopies,
      })

      const obj = store.getObject('default', 'objstyle.txt')
      expect(obj?.pieceCid).toBe('baga-obj')

      const copies = store.getObjectCopies('default', 'objstyle.txt')
      expect(copies).toHaveLength(2)
    })
  })

  // ── copyObject ──────────────────────────────────────────────────────

  describe('copyObject', () => {
    const copies = [
      {
        providerId: '42',
        dataSetId: '100',
        pieceId: '1000',
        retrievalUrl: 'https://sp1.example.com/baga1',
        role: 'primary' as const,
      },
    ]

    it('copies within same bucket', () => {
      store.putObject('default', 'src.txt', 'cid1', 100, 'text/plain', 'etag1', copies)

      const result = store.copyObject('default', 'src.txt', 'default', 'dst.txt')

      expect(result).toBeDefined()
      expect(result?.key).toBe('dst.txt')
      expect(result?.pieceCid).toBe('cid1')
      expect(result?.size).toBe(100)

      // Source still exists
      expect(store.getObject('default', 'src.txt')).toBeDefined()
    })

    it('copies across buckets', () => {
      store.createBucket('archive')
      store.putObject('default', 'file.txt', 'cid2', 200, 'text/plain', 'etag2')

      const result = store.copyObject('default', 'file.txt', 'archive', 'file.txt')

      expect(result).toBeDefined()
      expect(store.getObject('archive', 'file.txt')?.pieceCid).toBe('cid2')
    })

    it('preserves copy info (provider records)', () => {
      store.putObject('default', 'with-copies.txt', 'cid3', 50, 'text/plain', 'etag3', copies)

      store.copyObject('default', 'with-copies.txt', 'default', 'cloned.txt')

      const dstCopies = store.getObjectCopies('default', 'cloned.txt')
      expect(dstCopies).toHaveLength(1)
      expect(dstCopies[0]?.providerId).toBe('42')
      expect(dstCopies[0]?.retrievalUrl).toBe('https://sp1.example.com/baga1')
    })

    it('returns undefined for non-existent source', () => {
      const result = store.copyObject('default', 'no-such-file.txt', 'default', 'dst.txt')
      expect(result).toBeUndefined()
    })

    it('overwrites existing destination', () => {
      store.putObject('default', 'old.txt', 'cid-old', 10, 'text/plain', 'etag-old')
      store.putObject('default', 'new.txt', 'cid-new', 20, 'text/plain', 'etag-new')

      store.copyObject('default', 'new.txt', 'default', 'old.txt')

      expect(store.getObject('default', 'old.txt')?.pieceCid).toBe('cid-new')
    })

    it('returns undefined when destination bucket does not exist', () => {
      store.putObject('default', 'src.txt', 'cid-src', 100, 'text/plain', 'etag-src')

      const result = store.copyObject('default', 'src.txt', 'missing-bucket', 'dst.txt')

      expect(result).toBeUndefined()
      expect(store.getObject('missing-bucket', 'dst.txt')).toBeUndefined()
    })
  })

  // ── Basic CRUD ──────────────────────────────────────────────────────

  describe('putObject / getObject', () => {
    it('stores and retrieves an object', () => {
      store.putObject('default', 'file.txt', 'baga123', 1024, 'text/plain', 'etag1')

      const obj = store.getObject('default', 'file.txt')

      expect(obj).toBeDefined()
      expect(obj?.key).toBe('file.txt')
      expect(obj?.size).toBe(1024)
      expect(obj?.pieceCid).toBe('baga123')
      expect(obj?.contentType).toBe('text/plain')
      expect(obj?.etag).toBe('etag1')
    })

    it('returns undefined for non-existent key', () => {
      const obj = store.getObject('default', 'missing.txt')
      expect(obj).toBeUndefined()
    })

    it('overwrites existing object on re-put', () => {
      store.putObject('default', 'file.txt', 'baga-old', 100, 'text/plain', 'etag-old')
      store.putObject('default', 'file.txt', 'baga-new', 2048, 'application/pdf', 'etag-new')

      const obj = store.getObject('default', 'file.txt')

      expect(obj?.pieceCid).toBe('baga-new')
      expect(obj?.size).toBe(2048)
      expect(obj?.contentType).toBe('application/pdf')
      expect(obj?.etag).toBe('etag-new')
    })

    it('isolates objects across different buckets', () => {
      store.putObject('bucket-a', 'file.txt', 'cid-a', 100, 'text/plain', 'ea')
      store.putObject('bucket-b', 'file.txt', 'cid-b', 200, 'text/plain', 'eb')

      expect(store.getObject('bucket-a', 'file.txt')?.pieceCid).toBe('cid-a')
      expect(store.getObject('bucket-b', 'file.txt')?.pieceCid).toBe('cid-b')
    })
  })

  // ── Delete ──────────────────────────────────────────────────────────

  describe('deleteObject', () => {
    it('soft-deletes an object', () => {
      store.putObject('default', 'file.txt', 'cid', 100, 'text/plain', 'etag')

      const deleted = store.deleteObject('default', 'file.txt')

      expect(deleted).toBe(true)
      expect(store.getObject('default', 'file.txt')).toBeUndefined()
    })

    it('returns false when deleting non-existent key', () => {
      const deleted = store.deleteObject('default', 'missing.txt')
      expect(deleted).toBe(false)
    })

    it('returns false when deleting already-deleted key', () => {
      store.putObject('default', 'file.txt', 'cid', 100, 'text/plain', 'etag')
      store.deleteObject('default', 'file.txt')

      const deleted = store.deleteObject('default', 'file.txt')
      expect(deleted).toBe(false)
    })

    it('re-put after delete resurrects the object', () => {
      store.putObject('default', 'file.txt', 'cid-old', 100, 'text/plain', 'etag1')
      store.deleteObject('default', 'file.txt')
      store.putObject('default', 'file.txt', 'cid-new', 200, 'text/plain', 'etag2')

      const obj = store.getObject('default', 'file.txt')
      expect(obj).toBeDefined()
      expect(obj?.pieceCid).toBe('cid-new')
    })
  })

  // ── objectExists ────────────────────────────────────────────────────

  describe('objectExists', () => {
    it('returns true for existing object', () => {
      store.putObject('default', 'file.txt', 'cid', 100, 'text/plain', 'etag')
      expect(store.objectExists('default', 'file.txt')).toBe(true)
    })

    it('returns false for non-existent object', () => {
      expect(store.objectExists('default', 'missing.txt')).toBe(false)
    })

    it('returns false for deleted object', () => {
      store.putObject('default', 'file.txt', 'cid', 100, 'text/plain', 'etag')
      store.deleteObject('default', 'file.txt')
      expect(store.objectExists('default', 'file.txt')).toBe(false)
    })
  })

  // ── listObjects ─────────────────────────────────────────────────────

  describe('listObjects', () => {
    beforeEach(() => {
      // Populate test data — typical file tree
      const files = [
        'photos/2024/jan/a.jpg',
        'photos/2024/jan/b.jpg',
        'photos/2024/feb/c.jpg',
        'photos/2025/mar/d.jpg',
        'documents/report.pdf',
        'documents/notes.txt',
        'readme.md',
      ]
      for (const [i, key] of files.entries()) {
        store.putObject('default', key, `cid-${i}`, (i + 1) * 100, 'application/octet-stream', `etag-${i}`)
      }
    })

    it('lists all objects without prefix', () => {
      const { objects, isTruncated } = store.listObjects('default', '', '', 1000)

      expect(objects).toHaveLength(7)
      expect(isTruncated).toBe(false)
    })

    it('filters by prefix', () => {
      const { objects } = store.listObjects('default', 'photos/', '', 1000)

      expect(objects).toHaveLength(4)
      expect(objects.every((o) => o.key.startsWith('photos/'))).toBe(true)
    })

    it('filters by nested prefix', () => {
      const { objects } = store.listObjects('default', 'photos/2024/jan/', '', 1000)

      expect(objects).toHaveLength(2)
    })

    it('returns sorted results', () => {
      const { objects } = store.listObjects('default', '', '', 1000)

      const keys = objects.map((o) => o.key)
      const sorted = [...keys].sort()
      expect(keys).toEqual(sorted)
    })

    it('truncates at maxKeys', () => {
      const { objects, isTruncated } = store.listObjects('default', '', '', 3)

      expect(objects).toHaveLength(3)
      expect(isTruncated).toBe(true)
    })

    it('paginates with startAfter', () => {
      const page1 = store.listObjects('default', '', '', 3)
      const lastKey = page1.objects[page1.objects.length - 1]?.key

      const page2 = store.listObjects('default', '', '', 3, lastKey)

      expect(page2.objects).toHaveLength(3)
      expect(lastKey).toBeDefined()
      if (lastKey === undefined) {
        throw new Error('expected lastKey from first page')
      }
      // Page 2 keys should all be after page 1 last key
      for (const obj of page2.objects) {
        expect(obj.key > lastKey).toBe(true)
      }
    })

    it('groups by delimiter — top-level folders', () => {
      const { objects, commonPrefixes } = store.listObjects('default', '', '/', 1000)

      // Only "readme.md" is a top-level file
      expect(objects).toHaveLength(1)
      expect(objects[0]?.key).toBe('readme.md')

      // Folders become common prefixes
      expect(commonPrefixes).toContain('photos/')
      expect(commonPrefixes).toContain('documents/')
      expect(commonPrefixes).toHaveLength(2)
    })

    it('groups by delimiter — with prefix to list subfolder', () => {
      const { objects, commonPrefixes } = store.listObjects('default', 'photos/', '/', 1000)

      // No direct files under photos/ (all in subfolders)
      expect(objects).toHaveLength(0)

      // Subfolders become common prefixes
      expect(commonPrefixes).toContain('photos/2024/')
      expect(commonPrefixes).toContain('photos/2025/')
    })

    it('groups by delimiter — deeper prefix', () => {
      const { objects, commonPrefixes } = store.listObjects('default', 'photos/2024/', '/', 1000)

      expect(objects).toHaveLength(0)
      expect(commonPrefixes).toContain('photos/2024/jan/')
      expect(commonPrefixes).toContain('photos/2024/feb/')
    })

    it('returns empty for prefix with no matches', () => {
      const { objects, commonPrefixes } = store.listObjects('default', 'nonexistent/', '', 1000)

      expect(objects).toHaveLength(0)
      expect(commonPrefixes).toHaveLength(0)
    })

    it('excludes soft-deleted objects', () => {
      store.deleteObject('default', 'readme.md')

      const { objects } = store.listObjects('default', '', '', 1000)

      expect(objects).toHaveLength(6)
      expect(objects.find((o) => o.key === 'readme.md')).toBeUndefined()
    })

    it('returns empty for non-existent bucket', () => {
      const { objects } = store.listObjects('nonexistent-bucket', '', '', 1000)

      expect(objects).toHaveLength(0)
    })
  })
})
