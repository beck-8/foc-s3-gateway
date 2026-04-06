import { describe, expect, it } from 'vitest'
import { EncryptionService } from './encryption-service.js'

describe('Encryption integration', () => {
  const secretKey = 'test-secret-key-for-integration'

  it('full round-trip: encrypt → verify ciphertext differs from plaintext → decrypt → match', async () => {
    const service = new EncryptionService({ secretKey })
    await service.init()
    const salt = service.getSalt()

    // Simulate server restart: same salt used to re-derive the same key
    const service2 = new EncryptionService({ secretKey, salt })
    await service2.init()

    // Test with > 256 KiB for chunked encryption
    const plaintext = new Uint8Array(512 * 1024) // 512 KiB
    for (let i = 0; i < plaintext.length; i++) {
      plaintext[i] = i % 256
    }

    const encrypted = await service.encryptBuffer(plaintext)

    // Encrypted data should not contain the plaintext bytes
    const plaintextHex = Buffer.from(plaintext.slice(0, 100)).toString('hex')
    const encryptedHex = Buffer.from(encrypted).toString('hex')
    expect(encryptedHex).not.toContain(plaintextHex)

    // Metadata should reflect chunked encryption
    const meta = await service.getEncryptionMeta(encrypted)
    expect(meta.algorithm).toBe(-65793) // Chunked AES-256-GCM-STREAM
    // 512 KiB / 256 KiB = 2 chunks
    expect(meta.chunkCount).toBe(2)

    // Decrypt with service2 (same key derived from same salt — simulates server restart)
    const decrypted = await service2.decryptBuffer(encrypted)
    expect(decrypted).toEqual(plaintext)
  })

  it('wrong secret key cannot decrypt', async () => {
    const service = new EncryptionService({ secretKey })
    await service.init()

    const plaintext = new Uint8Array(256)
    for (let i = 0; i < plaintext.length; i++) {
      plaintext[i] = i % 256
    }

    const encrypted = await service.encryptBuffer(plaintext)

    // Different secret key, same salt
    const wrongService = new EncryptionService({ secretKey: 'wrong-key', salt: service.getSalt() })
    await wrongService.init()

    await expect(wrongService.decryptBuffer(encrypted)).rejects.toThrow()
  })

  it('range decryption across chunk boundary returns correct plaintext', async () => {
    const service = new EncryptionService({ secretKey })
    await service.init()

    // 512 KiB → 2 chunks of 256 KiB each
    const plaintext = new Uint8Array(512 * 1024)
    for (let i = 0; i < plaintext.length; i++) {
      plaintext[i] = i % 256
    }

    const encrypted = await service.encryptBuffer(plaintext)

    // BlobFetcher backed by the in-memory encrypted blob
    const fetcher = {
      async fetchEnvelope() {
        return encrypted.slice(0, 4096)
      },
      async fetchRange(offset: number, length: number) {
        return encrypted.slice(offset, offset + length)
      },
    }

    const metadata = await service.parseEnvelope(fetcher)
    expect(metadata.seekable).toBe(true)

    // Range that spans the chunk boundary (chunk 0 ends at byte 262143, chunk 1 starts at 262144)
    const rangeResult = await service.decryptRange(fetcher, metadata, {
      offset: 262000,
      length: 1000,
    })

    expect(rangeResult.length).toBe(1000)
    expect(rangeResult).toEqual(plaintext.slice(262000, 263000))
  })

  it('small file (< 256 KiB) uses non-seekable encryption', async () => {
    const service = new EncryptionService({ secretKey })
    await service.init()

    const plaintext = new Uint8Array(200)
    for (let i = 0; i < plaintext.length; i++) {
      plaintext[i] = i % 256
    }

    const encrypted = await service.encryptBuffer(plaintext)
    const meta = await service.getEncryptionMeta(encrypted)

    expect(meta.algorithm).toBe(3) // AES-256-GCM (non-seekable)
    expect(meta.chunkSize).toBeUndefined()
    expect(meta.chunkCount).toBeUndefined()

    const decrypted = await service.decryptBuffer(encrypted)
    expect(decrypted).toEqual(plaintext)
  })

  it('different salt produces different ciphertext', async () => {
    const service1 = new EncryptionService({ secretKey })
    await service1.init()

    const service2 = new EncryptionService({ secretKey }) // new salt
    await service2.init()

    // Salts should differ
    expect(service1.getSalt()).not.toEqual(service2.getSalt())

    const plaintext = new Uint8Array(256)
    for (let i = 0; i < plaintext.length; i++) {
      plaintext[i] = i % 256
    }

    const enc1 = await service1.encryptBuffer(plaintext)
    const enc2 = await service2.encryptBuffer(plaintext)

    // Different keys → different ciphertext
    expect(enc1).not.toEqual(enc2)

    // Each can only decrypt its own ciphertext
    const dec1 = await service1.decryptBuffer(enc1)
    expect(dec1).toEqual(plaintext)

    await expect(service1.decryptBuffer(enc2)).rejects.toThrow()
  })
})
