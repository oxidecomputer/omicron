// Search on Pagefind (https://pagefind.app/). Its client is
// pagefind-client.ts.

import { fileURLToPath } from 'node:url'

import * as pagefind from 'pagefind'

import type { SearchPage, SearchProvider } from './types.ts'

/**
 * A page as Pagefind indexes it. Section headings carry their ID on an empty
 * span inside the heading. Move it to the heading itself, which is where
 * Pagefind looks for anchors when it splits a page into per-section results.
 * If react-asciidoc's markup changes and this stops matching, search results
 * lose their section links with no error, so warn.
 */
function pageHtml(page: SearchPage) {
  const body = page.html.replace(
    /<(h[1-6])([^>]*)><span class="anchor" id="([^"]+)" aria-hidden="true"><\/span>/g,
    '<$1 id="$3"$2>',
  )
  if (body.includes('<span class="anchor"')) {
    console.warn(`${page.url}: heading anchors not moved; Pagefind won't link to sections`)
  }
  return `<html lang="en"><body><h1>${page.title}</h1>${body}</body></html>`
}

export const pagefindSearch: SearchProvider = {
  async build(pages, dir) {
    const { index, errors } = await pagefind.createIndex()
    if (!index) throw new Error(`Pagefind: ${errors.join('\n')}`)
    const allErrors: string[] = []
    for (const page of pages) {
      const added = await index.addHTMLFile({ url: `/${page.url}`, content: pageHtml(page) })
      allErrors.push(...added.errors)
    }
    const written = await index.writeFiles({ outputPath: dir })
    await pagefind.close()
    allErrors.push(...written.errors)
    if (allErrors.length > 0) throw new Error(`Pagefind: ${allErrors.join('\n')}`)
  },
  client: fileURLToPath(new URL('pagefind-client.ts', import.meta.url)),
}
