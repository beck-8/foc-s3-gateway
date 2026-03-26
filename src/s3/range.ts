/**
 * HTTP Range header parser for S3 and WebDAV byte-range requests.
 *
 * Supports single byte ranges per RFC 7233:
 *   - bytes=start-end   (both specified)
 *   - bytes=start-      (from offset to end)
 *   - bytes=-suffix     (last N bytes)
 *
 * Multi-range requests (e.g. bytes=0-50, 100-150) are NOT supported —
 * S3 spec only requires single range support.
 */

export interface ByteRange {
  /** Inclusive start byte offset (0-indexed) */
  start: number
  /** Inclusive end byte offset (0-indexed) */
  end: number
}

/**
 * Parse an HTTP Range header into a resolved byte range.
 *
 * @param rangeHeader - Raw `Range` header value (e.g. "bytes=0-99")
 * @param totalSize   - Total size of the resource in bytes
 * @returns Resolved `{ start, end }` or `undefined` if the header is unparseable / not a byte range.
 *          Returns `'unsatisfiable'` if the range is valid syntax but cannot be satisfied
 *          (e.g. start >= totalSize).
 */
export function parseRangeHeader(rangeHeader: string, totalSize: number): ByteRange | 'unsatisfiable' | undefined {
  // Must start with "bytes="
  if (!rangeHeader.startsWith('bytes=')) return undefined

  const spec = rangeHeader.slice(6) // strip "bytes="

  // Reject multi-range (contains comma)
  if (spec.includes(',')) return undefined

  const dashIdx = spec.indexOf('-')
  if (dashIdx < 0) return undefined

  const startStr = spec.slice(0, dashIdx).trim()
  const endStr = spec.slice(dashIdx + 1).trim()

  // Empty file cannot satisfy any range
  if (totalSize === 0) return 'unsatisfiable'

  // suffix-byte-range: bytes=-500 → last 500 bytes
  if (startStr === '') {
    if (endStr === '') return undefined
    const suffix = Number.parseInt(endStr, 10)
    if (Number.isNaN(suffix) || suffix <= 0) return 'unsatisfiable'
    const start = Math.max(0, totalSize - suffix)
    return { start, end: totalSize - 1 }
  }

  const start = Number.parseInt(startStr, 10)
  if (Number.isNaN(start) || start < 0) return undefined

  // byte-range: bytes=500- → from 500 to end
  if (endStr === '') {
    if (start >= totalSize) return 'unsatisfiable'
    return { start, end: totalSize - 1 }
  }

  // byte-range: bytes=200-499
  const end = Number.parseInt(endStr, 10)
  if (Number.isNaN(end)) return undefined

  // S3 behavior: start > end is invalid
  if (start > end) return undefined

  // start beyond file → unsatisfiable
  if (start >= totalSize) return 'unsatisfiable'

  // Clamp end to file boundary
  return { start, end: Math.min(end, totalSize - 1) }
}
