import type { BlobFetcher } from 'foc-encryption'
import { CoseAlgorithm } from 'foc-encryption'
import { describe, expect, it } from 'vitest'
import { EncryptionService } from './encryption-service.js'

const SMALL_SIZE = 64 * 1024 // 64 KiB — below 256 KiB threshold
const LARGE_SIZE = 300 * 1024 // 300 KiB — above 256 KiB threshold

describe('EncryptionService', () => {
  describe('init and key derivation', () => {
    it('isReady() returns true after init()', async () => {
      const svc = new EncryptionService({ secretKey: 'test-secret' })
      expect(svc.isReady()).toBe(false)
      await svc.init()
      expect(svc.isReady()).toBe(true)
    })

    it('getSalt() returns a Uint8Array after init()', async () => {
      const svc = new EncryptionService({ secretKey: 'test-secret' })
      await svc.init()
      const salt = svc.getSalt()
      expect(salt).toBeInstanceOf(Uint8Array)
      expect(salt.length).toBeGreaterThan(0)
    })
  })

  describe('deterministic re-derivation with same key + salt', () => {
    it('encrypts with one service and decrypts with another using same key+salt', async () => {
      const svc1 = new EncryptionService({ secretKey: 'shared-secret' })
      await svc1.init()
      const salt = svc1.getSalt()

      const svc2 = new EncryptionService({ secretKey: 'shared-secret', salt })
      await svc2.init()

      const plaintext = new Uint8Array(SMALL_SIZE).fill(42)
      const encrypted = await svc1.encryptBuffer(plaintext)
      const decrypted = await svc2.decryptBuffer(encrypted)

      expect(decrypted).toEqual(plaintext)
    })
  })

  describe('small file round-trip (< 256 KiB)', () => {
    it('uses AES_256_GCM algorithm and round-trips correctly', async () => {
      const svc = new EncryptionService({ secretKey: 'small-file-key' })
      await svc.init()

      const plaintext = new Uint8Array(SMALL_SIZE)
      for (let i = 0; i < plaintext.length; i++) {
        plaintext[i] = i % 256
      }

      const encrypted = await svc.encryptBuffer(plaintext)
      const meta = await svc.getEncryptionMeta(encrypted)

      expect(meta.algorithm).toBe(CoseAlgorithm.AES_256_GCM) // 3

      const decrypted = await svc.decryptBuffer(encrypted)
      expect(decrypted).toEqual(plaintext)
    })
  })

  describe('large file round-trip (> 256 KiB)', () => {
    it('uses CHUNKED_AES_256_GCM_STREAM algorithm, has 2 chunks, and round-trips correctly', async () => {
      const svc = new EncryptionService({ secretKey: 'large-file-key' })
      await svc.init()

      const plaintext = new Uint8Array(LARGE_SIZE)
      for (let i = 0; i < plaintext.length; i++) {
        plaintext[i] = i % 256
      }

      const encrypted = await svc.encryptBuffer(plaintext)
      const meta = await svc.getEncryptionMeta(encrypted)

      expect(meta.algorithm).toBe(CoseAlgorithm.CHUNKED_AES_256_GCM_STREAM) // -65793
      expect(meta.chunkCount).toBe(2)

      const decrypted = await svc.decryptBuffer(encrypted)
      expect(decrypted).toEqual(plaintext)
    })
  })

  describe('range decryption', () => {
    it('decrypts a byte range matching the correct plaintext slice', async () => {
      const svc = new EncryptionService({ secretKey: 'range-decrypt-key' })
      await svc.init()

      const plaintext = new Uint8Array(LARGE_SIZE)
      for (let i = 0; i < plaintext.length; i++) {
        plaintext[i] = i % 256
      }

      const encrypted = await svc.encryptBuffer(plaintext)
      const meta = await svc.getEncryptionMeta(encrypted)
      const envelopeMetadata = svc.parseEnvelope(encrypted)

      const fetcher: BlobFetcher = {
        fetchEnvelope: async () => encrypted.slice(0, meta.envelopeSize),
        fetchRange: async (offset: number, length: number) => encrypted.slice(offset, offset + length),
      }

      const range = { offset: 1000, length: 100 }
      const decryptedRange = await svc.decryptRange(fetcher, envelopeMetadata, range)

      expect(decryptedRange).toEqual(plaintext.slice(1000, 1100))
    })
  })
})
