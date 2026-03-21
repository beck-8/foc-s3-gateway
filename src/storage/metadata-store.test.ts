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
      for (let i = 0; i < files.length; i++) {
        const key = files[i]!
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
      // Page 2 keys should all be after page 1 last key
      for (const obj of page2.objects) {
        expect(obj.key > lastKey!).toBe(true)
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
