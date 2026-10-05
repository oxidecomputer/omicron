// Lists the AsciiDoc and Markdown files in the repo that aren't in nav.ts,
// largest first, to help spot docs that might belong on the site. Run it by
// hand with `npm run unlisted`; the build doesn't check this.

import { execFileSync } from 'node:child_process'
import fs from 'node:fs'
import path from 'node:path'

import { site } from './nav.ts'

const repoRoot = path.resolve(import.meta.dirname, '../..')

const listed = new Set(
  site.sections.flatMap((s) => s.pages.map((p) => (typeof p === 'string' ? p : p.path))),
)

// Tracked and untracked-but-not-ignored files, so a new doc shows up before
// it's committed
const files = execFileSync(
  'git',
  ['ls-files', '--cached', '--others', '--exclude-standard', '*.adoc', '*.md'],
  { cwd: repoRoot, encoding: 'utf8' },
)
  .split('\n')
  .filter((f) => f && !listed.has(f) && !f.startsWith('.') && !f.startsWith('docs/site/'))
  .map((f) => ({
    path: f,
    lines: fs.readFileSync(path.join(repoRoot, f), 'utf8').trimEnd().split('\n').length,
  }))
  .sort((a, b) => b.lines - a.lines)

for (const f of files) {
  console.log(`${String(f.lines).padStart(5)}  ${f.path}`)
}
