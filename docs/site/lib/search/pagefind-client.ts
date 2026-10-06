// The client for Pagefind search (pagefind.ts): loading Pagefind's JS API, and
// turning a query into the pages and sections to show, in order.

import type { LoadEngine, Result, SectionResult } from './types.ts'

/** The parts of Pagefind's search API used here */
type Pagefind = {
  options(opts: { basePath: string; baseUrl: string }): Promise<void>
  init(): Promise<void>
  search(term: string): Promise<SearchResponse>
}
type SearchResponse = { results: { words: number[]; data(): Promise<ResultData> }[] }
type SubResult = SectionResult & { locations: number[] }
type ResultData = {
  url: string
  content: string
  excerpt: string
  meta: { title?: string }
  sub_results: SubResult[]
}
/** `words` are the indexes Pagefind matched in `tokens`, the page text split on whitespace */
type Match = ResultData & { words: number[]; tokens: string[] }

const parts = (s: string) =>
  s
    .toLowerCase()
    .split(/[^\p{L}\p{N}]+/u)
    .filter(Boolean)

function commonPrefix(a: string, b: string) {
  let i = 0
  while (i < a.length && i < b.length && a[i] === b[i]) i++
  return i
}

/**
 * Whether a page word counts as a match for a query term: they share at least
 * half the term's length, and at least 3 characters. Stemmed matches like
 * "installations" → "install" share enough.
 */
function termMatches(term: string, word: string) {
  return commonPrefix(term, word) >= Math.min(term.length, Math.max(3, Math.ceil(term.length / 2)))
}

/**
 * When no indexed word starts with a query term, Pagefind falls back to the
 * longest indexed word the term starts with, so "sdfsdf" matches every `-s`
 * flag on the site. Drop a result unless every term matches a word Pagefind
 * matched on the page, or a word of its title.
 */
function isRealMatch(terms: string[], r: Match) {
  const matched = [...r.words.map((i) => r.tokens[i] ?? ''), r.meta.title ?? ''].flatMap(parts)
  return terms.every((term) => matched.some((w) => termMatches(term, w)))
}

/** Whether the terms appear in order and adjacent in a title or heading */
function hasPhrase(terms: string[], heading: string) {
  const words = parts(heading)
  return words.some((_, start) => terms.every((t, i) => termMatches(t, words[start + i] ?? '')))
}

/**
 * Pagefind scores each term on its own and penalizes long pages, so "bad
 * update" ranks a short page that says "update" a lot over the long one with a
 * section called "Recovering from a bad update". For queries of two or more
 * words, pages and sections with the query as a phrase in their title or a
 * heading go first. A phrase in the body text is too weak a signal: it put a
 * passing mention of "run simulated omicron" ahead of the page on running
 * simulated Omicron.
 */
const sectionHasPhrase = (terms: string[], s: SubResult) =>
  terms.length > 1 && hasPhrase(terms, s.title)

const pageHasPhrase = (terms: string[], r: Match) =>
  (terms.length > 1 && hasPhrase(terms, r.meta.title ?? '')) ||
  r.sub_results.some((s) => sectionHasPhrase(terms, s))

/** Stable sort with the items `first` picks ahead of the rest */
function putFirst<T>(items: T[], first: (item: T) => boolean) {
  return [...items.filter(first), ...items.filter((item) => !first(item))]
}

/**
 * Sections to show under a page, like Pagefind's UI: skip the first one if
 * it's the page itself (text before the first heading), and keep the 3 with
 * the most matches, in page order. Sections with the query as a phrase in
 * their heading come before the rest.
 */
function sectionsToShow(terms: string[], r: Match) {
  const subs = r.sub_results[0]?.url === r.url ? r.sub_results.slice(1) : r.sub_results
  const byMatches = [...subs].sort((a, b) => b.locations.length - a.locations.length)
  const top = putFirst(byMatches, (s) => sectionHasPhrase(terms, s)).slice(0, 3)
  return subs.filter((s) => top.includes(s))
}

/** Pagefind's URLs start with `/`, its base URL */
const relative = (url: string) => url.slice(1)

async function search(pagefind: Pagefind, query: string): Promise<Result[]> {
  const response = await pagefind.search(query)
  const matches: Match[] = await Promise.all(
    response.results.map(async (r) => {
      const data = await r.data()
      return { ...data, words: r.words, tokens: data.content.split(/\s+/) }
    }),
  )
  const terms = parts(query)
  const real = matches.filter((r) => isRealMatch(terms, r))
  return putFirst(real, (r) => pageHasPhrase(terms, r)).map((r) => ({
    url: relative(r.url),
    title: r.meta.title ?? r.url,
    excerpt: r.excerpt,
    sections: sectionsToShow(terms, r).map((s) => ({
      title: s.title,
      url: relative(s.url),
      excerpt: s.excerpt,
    })),
  }))
}

export const load: LoadEngine = async (dir) => {
  const pagefind: Pagefind = await import(new URL('pagefind.js', dir).href)
  await pagefind.options({ basePath: dir.pathname, baseUrl: '/' })
  await pagefind.init()
  return { search: (query) => search(pagefind, query) }
}
