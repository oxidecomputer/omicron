// Builds the developer docs site: renders every page listed in nav.ts to
// static HTML in dist/, mirroring each page's path in the repo so relative
// links and images keep working.

import fs from 'node:fs'
import path from 'node:path'

import {
  AsciiDocBlocks,
  attrs,
  handleDocument,
  inlineOverrides,
  loadAsciidoctor,
} from '@oxide/design-system/asciidoc'
import { Asciidoc } from '@oxide/react-asciidoc'
import { oxideTheme } from '@oxide/design-system/syntax'
import GithubSlugger from 'github-slugger'
import { createMarkdownExit } from 'markdown-exit'
import type { ReactNode } from 'react'
import { renderToStaticMarkup } from 'react-dom/server'
import { bundledLanguages, createHighlighter, type BundledLanguage } from 'shiki'

import { DocPage, IndexPage } from './layout.tsx'
import { site } from './nav.ts'
import type { Page, Section, TocItem } from './types.ts'

const siteDir = import.meta.dirname
const repoRoot = path.resolve(siteDir, '../..')
const outDir = path.join(siteDir, 'dist')

const ad = loadAsciidoctor({})

async function renderAdoc(src: string) {
  const logger = ad.MemoryLogger.create()
  ad.LoggerManager.setLogger(logger)
  const doc = ad.loadFile(path.join(repoRoot, src), {
    standalone: true,
    safe: 'safe',
    attributes: attrs,
  })
  const document = await handleDocument(doc)
  for (const msg of logger.getMessages()) {
    console.warn(`${src}: asciidoctor ${msg.getSeverity()}: ${msg.getText()}`)
  }

  const toToc = (s: (typeof document.sections)[number]): TocItem => ({
    id: s.id,
    title: s.title,
    children: s.sections.map(toToc),
  })
  const html = renderToStaticMarkup(
    <Asciidoc
      document={document}
      options={{
        overrides: {
          admonition: AsciiDocBlocks.Admonition,
          section: AsciiDocBlocks.Section,
          table: AsciiDocBlocks.Table,
        },
        inlineOverrides,
        customDocument: AsciiDocBlocks.MinimalDocument,
      }}
    />,
  )
  // Section headings carry their ID on an empty span inside the heading. Move
  // it to the heading itself, which is where Pagefind looks for anchors when it
  // splits a page into per-section search results.
  // If react-asciidoc's markup changes and this stops matching, search results
  // quietly lose their section links, so say so.
  let moved = 0
  const body = html.replace(
    /<(h[1-6])([^>]*)><span class="anchor" id="([^"]+)" aria-hidden="true"><\/span>/g,
    (_, tag: string, rest: string, id: string) => {
      moved++
      return `<${tag} id="${id}"${rest}>`
    },
  )
  if (document.sections.length > 0 && moved === 0) {
    console.warn(`${src}: no heading anchors found; Pagefind won't link to sections`)
  }
  return { title: document.title, body, toc: document.sections.map(toToc) }
}

// Code blocks get the same shiki theme and markup as AsciiDoc listings, so
// they pick up the same styles. Languages load on demand in renderMarkdown.
const highlighter = await createHighlighter({ themes: [oxideTheme], langs: [] })

/** The language of a fenced code block: the first word of its info string */
const fenceLang = (info: string) => info.trim().split(/\s+/)[0]

function highlightFence(code: string, lang: string) {
  const resolved = highlighter.getLoadedLanguages().includes(lang) ? lang : 'text'
  const html = highlighter.codeToHtml(code.replace(/\n$/, ''), {
    lang: resolved,
    theme: oxideTheme,
    structure: 'inline',
  })
  return `<div class="listingblock"><div class="content"><pre class="highlight"><code class="language-${resolved}" data-lang="${resolved}">${html}</code></pre></div></div>\n`
}

async function renderMarkdown(src: string) {
  let title = ''
  const toc: TocItem[] = []
  // IDs match GitHub's, so links to Markdown headings written against GitHub
  // keep working
  const slugger = new GithubSlugger()

  const md = createMarkdownExit({ html: true, linkify: true })
  const tokens = md.parse(fs.readFileSync(path.join(repoRoot, src), 'utf8'), {})

  for (let i = 0; i < tokens.length; i++) {
    const token = tokens[i]
    if (token.type === 'fence') {
      const lang = fenceLang(token.info)
      if (Object.hasOwn(bundledLanguages, lang) && !highlighter.getLoadedLanguages().includes(lang)) {
        await highlighter.loadLanguage(lang as BundledLanguage)
      }
    }
    if (token.type !== 'heading_open') continue
    const depth = Number(token.tag.slice(1))
    const children = tokens[i + 1].children ?? []
    const text = md.renderer.renderInline(children, md.options, {})
    // The first h1 is the page title, which the layout renders itself
    if (depth === 1 && !title) {
      title = text
      tokens.splice(i, 3)
      i--
      continue
    }
    const id = slugger.slug(
      children
        .filter((t) => t.type === 'text' || t.type === 'code_inline')
        .map((t) => t.content)
        .join(''),
    )
    token.attrSet('id', id)
    if (depth === 2) toc.push({ id, title: text, children: [] })
    if (depth === 3) toc.at(-1)?.children.push({ id, title: text, children: [] })
  }

  md.renderer.rules.heading_open = (tokens, idx) => {
    const { tag, attrs } = tokens[idx]
    const id = attrs?.find(([name]) => name === 'id')?.[1]
    return `<${tag} id="${id}"><a class="anchor" href="#${id}"></a>`
  }
  md.renderer.rules.fence = (tokens, idx) =>
    highlightFence(tokens[idx].content, fenceLang(tokens[idx].info))

  const html = md.renderer.render(tokens, md.options, {})
  const body = `<div id="content" class="asciidoc-body w-full">${html}</div>`
  return { title, body, toc }
}

const outPath = (src: string) => src.replace(/\.(adoc|md)$/, '.html')

const sections: Section[] = []
for (const { pages: entries, ...meta } of site.sections) {
  const section: Section = { ...meta, pages: [] }
  sections.push(section)
  for (const entry of entries) {
    const { path: src, title } = typeof entry === 'string' ? { path: entry } : entry
    if (!fs.existsSync(path.join(repoRoot, src))) {
      console.warn(`nav.ts: ${src} does not exist, skipping`)
      continue
    }
    const rendered = src.endsWith('.md') ? await renderMarkdown(src) : await renderAdoc(src)
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
const pagesByOut = new Map(pages.map((p) => [p.out, p]))
const assets = new Set<string>()

/** Resolve a relative URL in `page` to a repo path, or undefined if external */
function resolve(page: Page, url: string) {
  if (/^([a-z][a-z0-9+.-]*:|#|\/)/i.test(url)) return undefined
  const [urlPath, hash = ''] = url.split(/(?=#)/)
  const target = path.posix.normalize(path.posix.join(path.posix.dirname(page.src), urlPath))
  if (target.startsWith('..')) return undefined
  return { target, hash, linked: pagesByOut.get(outPath(target)) }
}

/**
 * Point links at the right place. A link to another page on the site becomes a
 * relative link to its HTML; an image is copied into dist/ at the same relative
 * path; anything else in the repo links to the file on GitHub.
 */
function rewriteUrls(html: string, page: Page) {
  // An xref with no label, like xref:foo.adoc[], renders with the path as its
  // text. Use the linked page's title instead.
  html = html.replace(/<a href="([^"]*)">\1<\/a>/g, (match, url: string) => {
    const linked = resolve(page, url)?.linked
    return linked ? `<a href="${url}">${linked.title}</a>` : match
  })

  return html.replace(/(href|src)="([^"]*)"/g, (match, attr: string, url: string) => {
    const resolved = resolve(page, url)
    if (!resolved) return match
    const { target, hash, linked } = resolved

    if (linked) {
      const rel = path.posix.relative(path.posix.dirname(page.out), linked.out)
      return `${attr}="${rel}${hash}"`
    }

    // Asciidoctor turns xref:foo.adoc[] into foo.html. If foo.adoc exists but
    // isn't on the site, link to the source on GitHub instead.
    const srcTarget =
      target.endsWith('.html') && !fs.existsSync(path.join(repoRoot, target))
        ? target.replace(/\.html$/, '.adoc')
        : target
    if (!fs.existsSync(path.join(repoRoot, srcTarget))) {
      console.warn(`${page.src}: broken link ${url}`)
      // A GitHub page URL can't render as an image anyway
      if (attr === 'src') return match
    } else if (attr === 'src') {
      assets.add(srcTarget)
      return match
    }
    return `${attr}="${site.repo}/blob/main/${srcTarget}${hash}"`
  })
}

function writeHtml(out: string, element: ReactNode) {
  const file = path.join(outDir, out)
  fs.mkdirSync(path.dirname(file), { recursive: true })
  fs.writeFileSync(file, '<!doctype html>\n' + renderToStaticMarkup(element))
}

fs.rmSync(outDir, { recursive: true, force: true })

writeHtml('index.html', <IndexPage sections={sections} />)
for (const [i, page] of pages.entries()) {
  const body = rewriteUrls(page.body, page)
  writeHtml(
    page.out,
    <DocPage
      sections={sections}
      page={page}
      body={body}
      prev={pages[i - 1]}
      next={pages[i + 1]}
    />,
  )
}

for (const asset of assets) {
  fs.mkdirSync(path.join(outDir, path.dirname(asset)), { recursive: true })
  fs.copyFileSync(path.join(repoRoot, asset), path.join(outDir, asset))
}

// The Oxide fonts are licensed and not checked into this repo. CI copies them
// in from the console repo; locally, the site falls back to system fonts.
const fontsDir = path.join(siteDir, 'fonts')
if (fs.existsSync(fontsDir)) {
  fs.cpSync(fontsDir, path.join(outDir, 'fonts'), { recursive: true })
}

console.log(`Built ${pages.length} pages and ${assets.size} assets into ${outDir}`)
