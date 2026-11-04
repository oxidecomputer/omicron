// Search on MiniSearch (https://lucaong.github.io/minisearch/), which loads
// the whole index the first time search opens. Each section of a page is a
// document. Its client is minisearch-client.ts.

import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

import MiniSearch from 'minisearch'

import { indexOptions, type Doc } from './minisearch-client.ts'
import type { SearchProvider } from './types.ts'

const entities: Record<string, string> = {
  amp: '&',
  lt: '<',
  gt: '>',
  quot: '"',
  apos: "'",
  nbsp: ' ',
}

/** Tags that separate words. Any other tag, like `<code>` or `<a>`, can sit inside a word. */
const blockTag =
  /^(address|article|aside|blockquote|br|dd|details|div|dl|dt|figcaption|figure|footer|h[1-6]|header|hr|li|ol|p|pre|section|summary|table|tbody|td|tfoot|th|thead|tr|ul)$/i

/** The text of an HTML fragment, with whitespace collapsed */
function htmlText(html: string) {
  return html
    .replace(/<(script|style|svg)\b[\s\S]*?<\/\1>/gi, ' ')
    .replace(/<\/?([a-z][a-z0-9]*)\b[^>]*>/gi, (_, tag: string) => (blockTag.test(tag) ? ' ' : ''))
    .replace(/&(#x[0-9a-f]+|#\d+|[a-z]+);/gi, (m, e: string) =>
      e[0] !== '#'
        ? (entities[e.toLowerCase()] ?? m)
        : String.fromCodePoint(
            e[1] === 'x' || e[1] === 'X' ? parseInt(e.slice(2), 16) : Number(e.slice(1)),
          ),
    )
    .replace(/\s+/g, ' ')
    .trim()
}

/**
 * Split a page body into sections at headings with an id. AsciiDoc headings
 * carry it on an anchor span inside the heading, and Markdown headings on the
 * heading itself. The first section, the text before the first heading, has no
 * anchor or heading.
 */
function splitSections(body: string) {
  const sections: Omit<Doc, 'id' | 'url' | 'pageTitle'>[] = []
  let current = { anchor: '', heading: '' }
  let last = 0
  for (const m of body.matchAll(/<h([2-6])\b([^>]*)>([\s\S]*?)<\/h\1>/g)) {
    const anchor = (m[2] + m[3]).match(/\bid="([^"]+)"/)?.[1]
    if (!anchor) continue
    sections.push({ ...current, text: htmlText(body.slice(last, m.index)) })
    current = { anchor, heading: htmlText(m[3]) }
    last = m.index + m[0].length
  }
  sections.push({ ...current, text: htmlText(body.slice(last)) })
  return sections.filter((s) => s.heading || s.text)
}

/** A module's file in node_modules */
const modulePath = (name: string) => fileURLToPath(import.meta.resolve(name))

export const minisearchSearch: SearchProvider = {
  build(pages, dir) {
    const docs: Doc[] = pages
      .flatMap((page) =>
        splitSections(page.html).map((s) => ({
          ...s,
          url: page.url,
          pageTitle: htmlText(page.title),
        })),
      )
      .map((doc, id) => ({ ...doc, id }))
    const index = new MiniSearch<Doc>(indexOptions)
    index.addAll(docs)
    fs.writeFileSync(path.join(dir, 'index.json'), JSON.stringify(index))
  },
  client: fileURLToPath(new URL('minisearch-client.ts', import.meta.url)),
  modules: { minisearch: modulePath('minisearch'), stemmer: modulePath('stemmer') },
}
