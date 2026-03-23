---
name: writing-tests
description: How to write and run tests for the FOC S3 Gateway
---

# Writing Tests

## Framework & Config

- **Vitest** with `globals: true` (no need to import `describe`, `it`, `expect`)
- Config: `vitest.config.ts` — includes `src/**/*.test.ts`
- Tests are co-located with source: `foo.ts` → `foo.test.ts`

## Running Tests

```bash
npm run test:unit       # Vitest only
npm test                # lint + typecheck + vitest (full suite)
npm run test:watch      # Vitest in watch mode
```

## Test Patterns by Component

### S3 XML Builders (`src/s3/xml.test.ts`)

Test XML output structure and content:

```typescript
import { buildSomeXml } from './xml.js'

describe('buildSomeXml', () => {
  it('should include required XML elements', () => {
    const xml = buildSomeXml({ key: 'value' })
    expect(xml).toContain('<?xml version="1.0"')
    expect(xml).toContain('<Key>value</Key>')
  })

  it('should escape special characters', () => {
    const xml = buildSomeXml({ key: 'a&b<c' })
    expect(xml).toContain('a&amp;b&lt;c')
  })

  it('should format dates as ISO 8601', () => {
    const xml = buildSomeXml({ date: '2026-03-21 13:57:52' })
    expect(xml).toContain('2026-03-21T13:57:52.000Z')
  })
})
```

### MetadataStore (`src/storage/metadata-store.test.ts`)

Use in-memory SQLite for fast, isolated tests:

```typescript
import { MetadataStore } from './metadata-store.js'
import pino from 'pino'

describe('MetadataStore - feature name', () => {
  let store: MetadataStore

  beforeEach(() => {
    store = new MetadataStore({
      dbPath: ':memory:',
      logger: pino({ level: 'silent' }),
    })
  })

  it('should create and retrieve objects', () => {
    store.createBucket('test')
    store.putObject('test', 'key', 'bafk...', 1024, 'text/plain', 'abc123')

    const obj = store.getObject('test', 'key')
    expect(obj).toBeDefined()
    expect(obj?.size).toBe(1024)
    expect(obj?.pieceCid).toBe('bafk...')
  })

  it('should handle soft delete', () => {
    store.createBucket('test')
    store.putObject('test', 'key', 'bafk...', 100, 'text/plain', 'etag')
    store.deleteObject('test', 'key')

    const obj = store.getObject('test', 'key')
    expect(obj).toBeUndefined()  // deleted=1 is filtered
  })

  it('should stage objects for async upload', () => {
    store.createBucket('test')
    store.stageObject('test', 'key', 500, 'text/plain', 'etag', '/tmp/staging/abc')

    const pending = store.getPendingUploads(10)
    expect(pending).toHaveLength(1)
    expect(pending[0]?.key).toBe('key')
  })
})
```

### WebDAV Routes (`src/webdav/routes.test.ts`)

Test path parsing and route behavior:

```typescript
describe('parseDavPath', () => {
  it('should parse root', () => {
    expect(parseDavPath('/')).toEqual({})
  })

  it('should parse bucket level', () => {
    expect(parseDavPath('/my-bucket/')).toEqual({ bucket: 'my-bucket' })
  })

  it('should parse file level', () => {
    expect(parseDavPath('/bucket/path/to/file.txt')).toEqual({
      bucket: 'bucket',
      key: 'path/to/file.txt',
    })
  })
})
```

## Best Practices

1. **Silent logger**: Use `pino({ level: 'silent' })` to suppress log output in tests
2. **In-memory DB**: Use `':memory:'` for MetadataStore — each test gets a fresh database
3. **No mocks for SQLite**: MetadataStore tests use real SQLite (in-memory), not mocks
4. **Test edge cases**: Empty keys, trailing slashes, special characters in keys, bucket boundaries
5. **Import extensions**: Always use `.js` — `import { foo } from './bar.js'`

## Adding a New Test File

1. Create `src/component/feature.test.ts` next to the source file
2. Vitest auto-discovers files matching `src/**/*.test.ts`
3. Use globals (`describe`, `it`, `expect`) — no import needed
4. Run `npm run test:unit` to verify
