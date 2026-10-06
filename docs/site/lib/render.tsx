// Renders one AsciiDoc or Markdown file to a page title, HTML body, and table
// of contents. Both formats come out with the same markup for headings and
// code blocks, so the design system's AsciiDoc styles cover both.

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
import { renderToStaticMarkup } from 'react-dom/server'
import {
  bundledLanguages,
  createHighlighter,
  type BundledLanguage,
  type Highlighter,
} from 'shiki'

import type { TocItem } from './types.ts'

export type Rendered = { title: string; body: string; toc: TocItem[] }

/** Render the file at `src`, relative to `repoRoot` */
export function renderDoc(repoRoot: string, src: string): Promise<Rendered> {
  const file = path.join(repoRoot, src)
  return src.endsWith('.md') ? renderMarkdown(file) : renderAdoc(file, src)
}

const ad = loadAsciidoctor({})

async function renderAdoc(file: string, src: string): Promise<Rendered> {
  const logger = ad.MemoryLogger.create()
  ad.LoggerManager.setLogger(logger)
  const doc = ad.loadFile(file, {
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
// they pick up the same styles. Created on first use; languages load on demand
// in renderMarkdown.
let highlighter: Promise<Highlighter> | undefined
const getHighlighter = () =>
  (highlighter ??= createHighlighter({ themes: [oxideTheme], langs: [] }))

/** The language of a fenced code block: the first word of its info string */
const fenceLang = (info: string) => info.trim().split(/\s+/)[0]

function highlightFence(highlighter: Highlighter, code: string, lang: string) {
  const resolved = highlighter.getLoadedLanguages().includes(lang) ? lang : 'text'
  const html = highlighter.codeToHtml(code.replace(/\n$/, ''), {
    lang: resolved,
    theme: oxideTheme,
    structure: 'inline',
  })
  return `<div class="listingblock"><div class="content"><pre class="highlight"><code class="language-${resolved}" data-lang="${resolved}">${html}</code></pre></div></div>\n`
}

async function renderMarkdown(file: string): Promise<Rendered> {
  const highlighter = await getHighlighter()
  let title = ''
  const toc: TocItem[] = []
  // IDs match GitHub's, so links to Markdown headings written against GitHub
  // keep working
  const slugger = new GithubSlugger()

  const md = createMarkdownExit({ html: true, linkify: true })
  const tokens = md.parse(fs.readFileSync(file, 'utf8'), {})

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
    highlightFence(highlighter, tokens[idx].content, fenceLang(tokens[idx].info))

  const html = md.renderer.render(tokens, md.options, {})
  const body = `<div id="content" class="asciidoc-body w-full">${html}</div>`
  return { title, body, toc }
}
