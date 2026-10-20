// The client for MiniSearch search (minisearch.ts): the index options the
// build shares, loading the index, and turning a query into the pages and
// sections to show, in order.
//
// Each section of a page (the text under a heading, or before the first one)
// is its own document, and a page ranks by its best section.

import MiniSearch, { type Options, type SearchResult } from 'minisearch'
import { stemmer } from 'stemmer'

import type { LoadEngine, Result } from './types.ts'

/** One section of a page, as indexed */
export type Doc = {
  id: number
  /** The page's URL path relative to the site root, e.g. `docs/how-to-run/` */
  url: string
  /** The heading's id, or empty for the text before the first heading */
  anchor: string
  pageTitle: string
  /** Empty for the text before the first heading */
  heading: string
  text: string
}

type Index = MiniSearch<Doc>

const tokenize = (s: string) =>
  s
    .toLowerCase()
    .split(/[^\p{L}\p{N}]+/u)
    .filter(Boolean)

/** Porter stemming, so "installations" and "install" match */
const stem = (term: string) => stemmer(term.toLowerCase())

/**
 * Options for building and loading the index. Every query term matches as a
 * prefix, since results update as you type, and every term has to match. No
 * fuzzy matching: it makes nonsense like "sdfsdf" match real words.
 */
export const indexOptions: Options<Doc> = {
  fields: ['pageTitle', 'heading', 'text'],
  storeFields: ['url', 'anchor', 'pageTitle', 'heading', 'text'],
  tokenize,
  processTerm: stem,
  searchOptions: {
    prefix: true,
    combineWith: 'AND',
    boost: { pageTitle: 3, heading: 2, text: 1 },
  },
}

type Hit = SearchResult & Doc

/** Hits grouped by page, in order of each page's first hit */
function byPage(hits: Hit[]) {
  const pages = new Map<string, Hit[]>()
  for (const hit of hits) {
    const page = pages.get(hit.url)
    if (page) page.push(hit)
    else pages.set(hit.url, [hit])
  }
  return [...pages.values()]
}

const escapeHtml = (s: string) => s.replace(/[&<>"']/g, (c) => `&#${c.charCodeAt(0)};`)

/** How many words an excerpt shows, and how many of them come before the first match */
const EXCERPT_WORDS = 30
const EXCERPT_LEAD = 8

/**
 * About 30 words of `text` around its first match as HTML, with every word
 * that matched in `<mark>`. `terms` are the stemmed index terms that matched,
 * from a hit's `terms`. If the match was only in a title or heading, this is
 * the start of the text.
 */
function excerpt(text: string, terms: string[]) {
  const words = text.split(' ')
  const matches = (word: string) => tokenize(word).some((t) => terms.includes(stem(t)))
  const first = words.findIndex(matches)
  const start = Math.max(0, first - EXCERPT_LEAD)
  return words
    .slice(start, start + EXCERPT_WORDS)
    .map((word) => (matches(word) ? `<mark>${escapeHtml(word)}</mark>` : escapeHtml(word)))
    .join(' ')
}

const sectionUrl = (hit: Hit) => (hit.anchor ? `${hit.url}#${hit.anchor}` : hit.url)

/**
 * A page's row, from its hits in score order. Its excerpt comes from the text
 * before the first heading if that matched, so it doesn't repeat the first
 * section shown under it, or else from its best section. The sections are its
 * 3 best that have a heading.
 */
function toResult(hits: Hit[]): Result {
  const intro = hits.find((h) => !h.heading) ?? hits[0]
  return {
    url: hits[0].url,
    title: hits[0].pageTitle,
    excerpt: excerpt(intro.text, intro.terms),
    sections: hits
      .filter((h) => h.heading)
      .slice(0, 3)
      .map((h) => ({ title: h.heading, url: sectionUrl(h), excerpt: excerpt(h.text, h.terms) })),
  }
}

/**
 * Search for `query`. Pages with a section that matches every term come
 * first, ranked by that section's score. After them come pages that only
 * match every term across sections, like "how-to-run helios", ranked by the
 * sum of their sections' scores.
 */
function search(index: Index, query: string): Result[] {
  const whole = byPage(index.search(query) as Hit[])
  const terms = new Set(tokenize(query).map(stem))
  if (terms.size < 2) return whole.map(toResult)

  const seen = new Set(whole.map((hits) => hits[0].url))
  const total = (hits: Hit[]) => hits.reduce((sum, h) => sum + h.score, 0)
  const spread = byPage(index.search(query, { combineWith: 'OR' }) as Hit[])
    .filter((hits) => !seen.has(hits[0].url))
    .filter((hits) => {
      const matched = new Set(hits.flatMap((h) => h.queryTerms))
      return [...terms].every((t) => matched.has(t))
    })
    .sort((a, b) => total(b) - total(a))
  return [...whole, ...spread].map(toResult)
}

export const load: LoadEngine = async (dir) => {
  const response = await fetch(new URL('index.json', dir))
  if (!response.ok) throw new Error(`Failed to load the search index: ${response.status}`)
  const index: Index = MiniSearch.loadJSON(await response.text(), indexOptions)
  return { search: (query) => search(index, query) }
}
