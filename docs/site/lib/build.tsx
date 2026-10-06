// Builds a docs site: renders every page listed in the site config to static
// HTML in outDir, mirroring each page's path in the repo so relative links and
// images keep working, then indexes it for search. Tailwind runs over the
// output afterward.

import fs from 'node:fs'
import { stripTypeScriptTypes } from 'node:module'
import path from 'node:path'

import type { ReactNode } from 'react'
import { renderToStaticMarkup } from 'react-dom/server'

import { DocPage, IndexPage } from './layout.tsx'
import { createLinkRewriter, outPath } from './links.ts'
import { renderDoc } from './render.tsx'
import type { SearchPage, SearchProvider } from './search/types.ts'
import type { Section, Site } from './types.ts'

export type BuildOptions = {
  site: Site
  /** Paths in the site config are relative to this */
  repoRoot: string
  /** Emptied and rewritten on every build */
  outDir: string
  /** Copied to outDir/fonts if it exists */
  fontsDir?: string
}

export async function buildSite({ site, repoRoot, outDir, fontsDir }: BuildOptions) {
  const sections: Section[] = []
  for (const { pages: entries, ...meta } of site.sections) {
    const section: Section = { ...meta, pages: [] }
    sections.push(section)
    for (const entry of entries) {
      const { path: src, title } = typeof entry === 'string' ? { path: entry } : entry
      if (!fs.existsSync(path.join(repoRoot, src))) {
        console.warn(`${src} is in the site config but does not exist, skipping`)
        continue
      }
      const rendered = await renderDoc(repoRoot, src)
      section.pages.push({
        ...rendered,
        src,
        out: outPath(src),
        section: section.title,
        title: title ?? (rendered.title || src),
      })
    }
  }
  const pages = sections.flatMap((s) => s.pages)
  // foo.adoc and foo/README.md would both be foo/. The build also writes the
  // search index and fonts to directories of their own.
  const bySrc = new Map([
    ['search/', 'the search index'],
    ['fonts/', 'the fonts'],
  ])
  for (const { out, src } of pages) {
    const other = bySrc.get(out)
    if (other) throw new Error(`${other} and ${src} would both be published at ${out}`)
    bySrc.set(out, src)
  }

  const writeHtml = (out: string, element: ReactNode) => {
    const file = path.join(outDir, out)
    fs.mkdirSync(path.dirname(file), { recursive: true })
    fs.writeFileSync(file, '<!doctype html>\n' + renderToStaticMarkup(element))
  }

  fs.rmSync(outDir, { recursive: true, force: true })

  const links = createLinkRewriter({ site, repoRoot, pages })
  writeHtml('index.html', <IndexPage site={site} sections={sections} />)
  const searchPages: SearchPage[] = []
  for (const [i, page] of pages.entries()) {
    const body = links.rewrite(page.body, page)
    searchPages.push({ url: page.out, title: page.title, html: body })
    writeHtml(
      `${page.out}index.html`,
      <DocPage
        site={site}
        sections={sections}
        page={page}
        body={body}
        prev={pages[i - 1]}
        next={pages[i + 1]}
      />,
    )
  }

  for (const asset of links.assets) {
    fs.mkdirSync(path.join(outDir, path.dirname(asset)), { recursive: true })
    fs.copyFileSync(path.join(repoRoot, asset), path.join(outDir, asset))
  }

  if (fontsDir && fs.existsSync(fontsDir)) {
    fs.cpSync(fontsDir, path.join(outDir, 'fonts'), { recursive: true })
  }

  if (site.search) await writeSearch(site.search, searchPages, outDir)

  console.log(`Built ${pages.length} pages and ${links.assets.size} assets into ${outDir}`)
}

/**
 * Index the pages with the site's search provider into outDir/search, and
 * write the scripts the browser runs: the search modal, and the provider's
 * client. Both only need their types removed (tsconfig's erasableSyntaxOnly
 * keeps them to syntax that allows that) since they only import types.
 */
async function writeSearch(provider: SearchProvider, pages: SearchPage[], outDir: string) {
  const dir = path.join(outDir, 'search')
  fs.mkdirSync(dir, { recursive: true })
  await provider.build(pages, dir)
  const strip = (file: string) => stripTypeScriptTypes(fs.readFileSync(file, 'utf8'))
  fs.writeFileSync(
    path.join(outDir, 'search.js'),
    strip(path.join(import.meta.dirname, 'search.ts')),
  )
  fs.writeFileSync(path.join(dir, 'engine.js'), strip(provider.client))
}
