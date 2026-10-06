// Builds a docs site: renders every page listed in the site config to static
// HTML in outDir, mirroring each page's path in the repo so relative links and
// images keep working, then indexes it for search. Tailwind runs over the
// output afterward.

import fs from 'node:fs'
import path from 'node:path'

import * as pagefind from 'pagefind'
import type { ReactNode } from 'react'
import { renderToStaticMarkup } from 'react-dom/server'

import { DocPage, IndexPage } from './layout.tsx'
import { createLinkRewriter, outPath } from './links.ts'
import { renderDoc } from './render.tsx'
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
  // foo.adoc and foo/README.md would both be foo/
  const bySrc = new Map<string, string>()
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
  for (const [i, page] of pages.entries()) {
    writeHtml(
      `${page.out}index.html`,
      <DocPage
        site={site}
        sections={sections}
        page={page}
        body={links.rewrite(page.body, page)}
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

  await writeSearchIndex(outDir)

  console.log(`Built ${pages.length} pages and ${links.assets.size} assets into ${outDir}`)
}

/**
 * Index the HTML in `outDir` with Pagefind, which writes the index and the
 * search UI's script and styles to outDir/pagefind. Only the part of each page
 * marked `data-pagefind-body` is indexed, and pages without it are skipped.
 */
async function writeSearchIndex(outDir: string) {
  const { index, errors } = await pagefind.createIndex()
  if (!index) throw new Error(`Pagefind: ${errors.join('\n')}`)
  const added = await index.addDirectory({ path: outDir })
  const written = await index.writeFiles({ outputPath: path.join(outDir, 'pagefind') })
  await pagefind.close()
  const allErrors = [...added.errors, ...written.errors]
  if (allErrors.length > 0) throw new Error(`Pagefind: ${allErrors.join('\n')}`)
}
