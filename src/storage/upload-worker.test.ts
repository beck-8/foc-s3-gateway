import { PassThrough } from 'node:stream'
import pino from 'pino'
import { describe, expect, it, vi } from 'vitest'
import { UploadWorker } from './upload-worker.js'

describe('UploadWorker desired copies handling', () => {
  it('keeps local file and marks failed when returned copies are below desired', async () => {
    const metadataStore = {
      markUploading: vi.fn(),
      completeUpload: vi.fn(),
      markUploadFailed: vi.fn(),
      recordPartialUpload: vi.fn(),
    }
    const synapseClient = {
      upload: vi.fn().mockResolvedValue({
        pieceCid: 'cid-1',
        size: 256,
        copies: [
          {
            providerId: '42',
            dataSetId: '100',
            pieceId: '1',
            retrievalUrl: 'https://sp1.example.com/piece/cid-1',
            role: 'primary' as const,
          },
        ],
      }),
    }
    const localStore = {
      exists: vi.fn().mockReturnValue(true),
      createReadStream: vi.fn().mockImplementation(() => {
        const stream = new PassThrough()
        setTimeout(() => {
          stream.end(Buffer.from('x'.repeat(256)))
        }, 0)
        return stream
      }),
      delete: vi.fn(),
    }

    const worker = new UploadWorker({
      metadataStore: metadataStore as any,
      synapseClient: synapseClient as any,
      localStore: localStore as any,
      logger: pino({ level: 'silent' }),
    })

    await (worker as any).uploadOne({
      bucket: 'default',
      key: 'a.bin',
      size: 256,
      contentType: 'application/octet-stream',
      localPath: '/tmp/a',
      desiredCopies: 2,
    })

    expect(metadataStore.markUploading).toHaveBeenCalledOnce()
    expect(synapseClient.upload).toHaveBeenCalledTimes(1)
    expect(synapseClient.upload.mock.calls[0]?.[1]).toEqual({ copies: 2 })
    expect(metadataStore.recordPartialUpload).toHaveBeenCalledWith(
      'default',
      'a.bin',
      'cid-1',
      expect.any(Array),
      '/tmp/a'
    )
    expect(metadataStore.markUploadFailed).not.toHaveBeenCalled()
    expect(metadataStore.completeUpload).not.toHaveBeenCalled()
    expect(localStore.delete).toHaveBeenCalledWith('/tmp/a')
  })
})
