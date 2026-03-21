/**
 * WebDAV XML response builders.
 *
 * DAV uses the multistatus/response pattern for property queries.
 * We only implement the minimum subset needed for file manager clients.
 */

export interface DavResource {
  href: string
  displayName: string
  isCollection: boolean
  contentLength?: number
  contentType?: string
  lastModified?: string
  etag?: string
}

function escapeXml(str: string): string {
  return str
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&apos;')
}

function formatHttpDate(dateStr: string | undefined): string {
  if (!dateStr) return new Date().toUTCString()
  try {
    return new Date(dateStr).toUTCString()
  } catch {
    return new Date().toUTCString()
  }
}

function buildResourceXml(resource: DavResource): string {
  const resourceType = resource.isCollection ? '<D:resourcetype><D:collection/></D:resourcetype>' : '<D:resourcetype/>'

  const props = [
    `<D:displayname>${escapeXml(resource.displayName)}</D:displayname>`,
    resourceType,
    `<D:getlastmodified>${formatHttpDate(resource.lastModified)}</D:getlastmodified>`,
  ]

  if (!resource.isCollection) {
    props.push(`<D:getcontentlength>${resource.contentLength ?? 0}</D:getcontentlength>`)
    props.push(`<D:getcontenttype>${escapeXml(resource.contentType ?? 'application/octet-stream')}</D:getcontenttype>`)
    if (resource.etag) {
      props.push(`<D:getetag>"${escapeXml(resource.etag)}"</D:getetag>`)
    }
  }

  return `<D:response>
  <D:href>${escapeXml(resource.href)}</D:href>
  <D:propstat>
    <D:prop>
      ${props.join('\n      ')}
    </D:prop>
    <D:status>HTTP/1.1 200 OK</D:status>
  </D:propstat>
</D:response>`
}

export function buildMultistatusXml(resources: DavResource[]): string {
  const responses = resources.map(buildResourceXml).join('\n')

  return `<?xml version="1.0" encoding="UTF-8"?>
<D:multistatus xmlns:D="DAV:">
${responses}
</D:multistatus>`
}
