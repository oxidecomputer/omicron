// Run the site's search against a built site in Node and score it against
// cases.ts. Build first (`npm run build`), then `npm run search-eval`.
//
//   node search-eval/eval.ts [--dist DIR] [--verbose]
//   node search-eval/eval.ts --query 'bad update'   # show results and sections
//
// A case only checks that a good page is in the top few, so a change can pass
// every case and still make results worse. So each run also saves the top 3
// results for every case, with the sections shown under each, to
// last-run.json (gitignored), and prints every case whose top 3 changed since
// the previous run. Run it before and after changing the ranking.
//
// This loads the site's search provider's client, like the search modal does,
// with `fetch` stubbed to read the index from dist/. Pagefind's pagefind.js
// runs in Node as is: with no `window` it skips its Web Worker.

import fs from 'node:fs/promises'
import path from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import { parseArgs } from 'node:util'

import type { LoadEngine, Result } from '../lib/search/types.ts'
import { site } from '../nav.ts'
import { cases } from './cases.ts'

const { values: args } = parseArgs({
  options: {
    dist: { type: 'string', default: path.join(import.meta.dirname, '../dist') },
    query: { type: 'string' },
    verbose: { type: 'boolean', default: false },
  },
})

// Clients fetch by file URL, or by path for Pagefind, which fetches by its
// basePath, the search/ directory's path here
globalThis.fetch = (async (url: string | URL) => {
  const file = String(url).split('?')[0]
  return new Response(
    await fs.readFile(file.startsWith('file:') ? fileURLToPath(file) : decodeURIComponent(file)),
  )
}) as typeof fetch

if (!site.search) throw new Error('The site has no search provider')
const { load }: { load: LoadEngine } = await import(pathToFileURL(site.search.client).href)
const engine = await load(pathToFileURL(path.resolve(args.dist, 'search') + '/'))

/** Search, with URLs starting with `/` like the cases' */
const run = async (query: string) =>
  (await engine.search(query)).map((r) => ({ ...r, url: `/${r.url}` }))

const fmt = (r: Result) => `${r.url}  ${r.title}`

/** How many results to compare between runs */
const COMPARE_TOP = 3
const lastRunPath = path.join(import.meta.dirname, 'last-run.json')

if (args.query) {
  const results = await run(args.query)
  console.log(`${results.length} results`)
  for (const r of results) {
    console.log(fmt(r))
    for (const s of r.sections) console.log(`    ${s.title}`)
  }
  process.exit(0)
}

const failures: string[] = []
/** Each case's top results as lines: a page, then the sections under it */
const thisRun: Record<string, string[]> = {}
let checked = 0
for (const c of cases) {
  const results = await run(c.q)
  thisRun[c.q] = results
    .slice(0, COMPARE_TOP)
    .flatMap((r) => [fmt(r), ...r.sections.map((s) => `    ${s.title}`)])

  const k = c.k ?? 3
  const ok = c.none
    ? results.length === 0
    : c.top
      ? results.slice(0, k).some((r) => c.top!.includes(r.url))
      : null
  if (ok === null) continue
  checked++
  if (ok) continue
  const want = c.none ? 'no results' : `one of ${c.top!.join(', ')} in top ${k}`
  let line = `  ✗ ${JSON.stringify(c.q)}: want ${want}, got ${results.length}`
  if (c.note) line += ` (${c.note})`
  if (args.verbose)
    line += results
      .slice(0, k)
      .map((r) => '\n      ' + fmt(r))
      .join('')
  failures.push(line)
}

const lastRun: Record<string, string[]> | null = await fs
  .readFile(lastRunPath, 'utf8')
  .then(JSON.parse, () => null)
await fs.writeFile(lastRunPath, JSON.stringify(thisRun, null, 2))

console.log(failures.join('\n'))
console.log(`\n${checked - failures.length}/${checked} cases pass`)

if (lastRun) {
  const indent = (lines: string[]) => lines.map((l) => `      ${l}`).join('\n') || '      (none)'
  const changed = cases.filter(
    (c) => c.q in lastRun && lastRun[c.q].join('\n') !== thisRun[c.q].join('\n'),
  )
  for (const c of changed) {
    console.log(
      `\n  ${c.q}\n    before\n${indent(lastRun[c.q])}\n    after\n${indent(thisRun[c.q])}`,
    )
  }
  console.log(`\n${changed.length} cases changed their top ${COMPARE_TOP} since the last run`)
}
