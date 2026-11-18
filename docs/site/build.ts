// Builds the Omicron developer docs site into dist/. The pages are listed in
// nav.ts; the builder itself is in lib/.

import path from 'node:path'

import { buildSite } from './lib/build.tsx'
import { site } from './nav.ts'

const siteDir = import.meta.dirname

await buildSite({
  site,
  repoRoot: path.resolve(siteDir, '../..'),
  outDir: path.join(siteDir, 'dist'),
  // The Oxide fonts are licensed and not checked into this repo. CI copies them
  // in from the console repo; locally, the site falls back to system fonts.
  fontsDir: path.join(siteDir, 'fonts'),
})
