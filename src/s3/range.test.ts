/**
 * Tests for HTTP Range header parsing.
 */

import { describe, expect, it } from 'vitest'
import { parseRangeHeader } from './range.js'

describe('parseRangeHeader', () => {
  const SIZE = 1000 // 1000-byte file

  // ── Valid ranges ──────────────────────────────────────────────────

  it('parses bytes=0-99 (first 100 bytes)', () => {
    expect(parseRangeHeader('bytes=0-99', SIZE)).toEqual({ start: 0, end: 99 })
  })

  it('parses bytes=500-999 (last half)', () => {
    expect(parseRangeHeader('bytes=500-999', SIZE)).toEqual({ start: 500, end: 999 })
  })

  it('parses bytes=0-0 (single byte)', () => {
    expect(parseRangeHeader('bytes=0-0', SIZE)).toEqual({ start: 0, end: 0 })
  })

  it('parses bytes=999-999 (last byte)', () => {
    expect(parseRangeHeader('bytes=999-999', SIZE)).toEqual({ start: 999, end: 999 })
  })

  // ── Open-ended ranges ────────────────────────────────────────────

  it('parses bytes=500- (from offset to end)', () => {
    expect(parseRangeHeader('bytes=500-', SIZE)).toEqual({ start: 500, end: 999 })
  })

  it('parses bytes=0- (entire file)', () => {
    expect(parseRangeHeader('bytes=0-', SIZE)).toEqual({ start: 0, end: 999 })
  })

  // ── Suffix ranges ────────────────────────────────────────────────

  it('parses bytes=-100 (last 100 bytes)', () => {
    expect(parseRangeHeader('bytes=-100', SIZE)).toEqual({ start: 900, end: 999 })
  })

  it('parses bytes=-1000 (entire file via suffix)', () => {
    expect(parseRangeHeader('bytes=-1000', SIZE)).toEqual({ start: 0, end: 999 })
  })

  it('parses bytes=-2000 (suffix larger than file → clamps to 0)', () => {
    expect(parseRangeHeader('bytes=-2000', SIZE)).toEqual({ start: 0, end: 999 })
  })

  // ── End clamping ─────────────────────────────────────────────────

  it('clamps end to file size - 1', () => {
    expect(parseRangeHeader('bytes=0-5000', SIZE)).toEqual({ start: 0, end: 999 })
  })

  it('clamps end when partially out of bounds', () => {
    expect(parseRangeHeader('bytes=900-1500', SIZE)).toEqual({ start: 900, end: 999 })
  })

  // ── Unsatisfiable ranges ─────────────────────────────────────────

  it('returns unsatisfiable when start >= totalSize', () => {
    expect(parseRangeHeader('bytes=1000-1500', SIZE)).toBe('unsatisfiable')
  })

  it('returns unsatisfiable when start > totalSize (open-ended)', () => {
    expect(parseRangeHeader('bytes=1001-', SIZE)).toBe('unsatisfiable')
  })

  it('returns unsatisfiable for 0-byte file', () => {
    expect(parseRangeHeader('bytes=0-0', 0)).toBe('unsatisfiable')
  })

  it('returns unsatisfiable for bytes=-0', () => {
    expect(parseRangeHeader('bytes=-0', SIZE)).toBe('unsatisfiable')
  })

  // ── Invalid / unparseable headers ────────────────────────────────

  it('returns undefined for non-bytes unit', () => {
    expect(parseRangeHeader('items=0-5', SIZE)).toBeUndefined()
  })

  it('returns undefined for multi-range', () => {
    expect(parseRangeHeader('bytes=0-50, 100-150', SIZE)).toBeUndefined()
  })

  it('returns undefined for start > end', () => {
    expect(parseRangeHeader('bytes=500-100', SIZE)).toBeUndefined()
  })

  it('returns undefined for garbage', () => {
    expect(parseRangeHeader('bytes=abc-def', SIZE)).toBeUndefined()
  })

  it('returns undefined for empty spec', () => {
    expect(parseRangeHeader('bytes=-', SIZE)).toBeUndefined()
  })

  it('returns undefined for missing dash', () => {
    expect(parseRangeHeader('bytes=100', SIZE)).toBeUndefined()
  })
})
