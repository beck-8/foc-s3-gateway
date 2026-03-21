/** S3 types used across the gateway */

export interface S3Object {
  key: string
  size: number
  lastModified: string
  etag: string
  pieceCid: string
  contentType: string
}

export interface S3Bucket {
  name: string
  creationDate: string
}

export interface ListObjectsV2Request {
  bucket: string
  prefix?: string
  delimiter?: string
  maxKeys?: number
  continuationToken?: string
  startAfter?: string
}

export interface ListObjectsV2Response {
  name: string
  prefix: string
  maxKeys: number
  isTruncated: boolean
  contents: S3Object[]
  commonPrefixes: string[]
  keyCount: number
  nextContinuationToken?: string
}

export interface S3ErrorResponse {
  code: string
  message: string
  resource?: string | undefined
  requestId: string
}
