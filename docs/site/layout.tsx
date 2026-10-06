import path from 'node:path'

import type { ReactNode } from 'react'

import { site } from './nav.ts'
import type { Page, Section, TocItem } from './types.ts'

// Pagefind's search UI web components
declare module 'react' {
  namespace JSX {
    interface IntrinsicElements {
      'pagefind-modal-trigger': { placeholder?: string }
      'pagefind-modal': {}
    }
  }
}

/** Page titles can contain inline markup and entities, e.g. `<code>oxdb sql</code>` */
const plain = (html: string) =>
  html
    .replace(/<[^>]+>/g, '')
    .replace(/&#(\d+);/g, (_, n) => String.fromCodePoint(Number(n)))
    .replace(/&#x([0-9a-f]+);/gi, (_, n) => String.fromCodePoint(parseInt(n, 16)))
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&amp;/g, '&')

const Html = ({ html }: { html: string }) => <span dangerouslySetInnerHTML={{ __html: html }} />

/** Relative URL from the page at `from` to the file at `to`, both relative to dist/ */
const relHref = (from: string, to: string) => path.posix.relative(path.posix.dirname(from), to)

/** Relative path from the page at `from` to the site root, e.g. `../..` */
const rootFrom = (from: string) => relHref(from, '.') || '.'

function Shell({
  title,
  root,
  children,
}: {
  title: string
  root: string
  children: ReactNode
}) {
  return (
    <html lang="en">
      <head>
        <meta charSet="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>{title}</title>
        {/* Pagefind's CSS goes first so style.css can override its variables */}
        <link rel="stylesheet" href={`${root}/pagefind/pagefind-component-ui.css`} />
        <link rel="stylesheet" href={`${root}/style.css`} />
        {/* A classic script, not type="module", so Pagefind can find its bundle
            relative to the script's own URL, which works under any base path */}
        <script src={`${root}/pagefind/pagefind-component-ui.js`} defer />
        {/* Dark by default, like other Oxide sites, unless the OS asks for light */}
        <script>{`if (matchMedia('(prefers-color-scheme: light)').matches) document.documentElement.dataset.theme = 'light'`}</script>
      </head>
      <body className="bg-default text-default">
        <header className="bg-default border-secondary sticky top-0 z-10 flex h-14 items-center gap-3 border-b px-6">
          <a href={`${root}/index.html`} className="text-sans-semi-xl text-raise">
            {site.title}
          </a>
          <span className="text-mono-sm text-tertiary 600:inline hidden">developer docs</span>
          <div className="ml-auto flex items-center gap-6">
            <pagefind-modal-trigger placeholder="Search" />
            <a href={site.repo} className="text-mono-sm text-secondary hover:text-default">
              GitHub
            </a>
          </div>
        </header>
        <pagefind-modal />
        {children}
      </body>
    </html>
  )
}

function Nav({ sections, current }: { sections: Section[]; current: Page }) {
  return (
    <div className="space-y-6">
      {sections.map((section) => (
        <div key={section.title}>
          <div className="text-mono-xs text-tertiary mb-2">{section.title}</div>
          <ul className="space-y-1.5">
            {section.pages.map((p) => (
              <li key={p.out}>
                <a
                  href={relHref(current.out, p.out)}
                  aria-current={p === current ? 'page' : undefined}
                  className={
                    p === current
                      ? 'text-sans-md text-accent block leading-tight'
                      : 'text-sans-md text-secondary hover:text-default block leading-tight'
                  }
                >
                  {plain(p.title)}
                </a>
              </li>
            ))}
          </ul>
        </div>
      ))}
    </div>
  )
}

/**
 * Every page has the same sidebar, so restore its scroll position from the
 * previous page. That keeps the link you clicked where it was. If the current
 * page's link still isn't visible (you got here from a link in a page body, or
 * by loading the page directly), center it. Inline right after the sidebar so
 * it runs before the sidebar is painted at the wrong position. The key includes
 * the repo because every site on a GitHub Pages domain shares one origin.
 */
const sidebarScrollKey = JSON.stringify(`sidebar-scroll:${site.repo}`)
const sidebarScrollScript = `{
  const nav = document.getElementById('sidebar')
  try { nav.scrollTop = Number(sessionStorage.getItem(${sidebarScrollKey})) } catch {}
  const n = nav.getBoundingClientRect()
  const l = nav.querySelector('[aria-current=page]').getBoundingClientRect()
  if (l.top < n.top || l.bottom > n.bottom) nav.scrollTop += l.top - n.top - (n.height - l.height) / 2
  addEventListener('pagehide', () => {
    try { sessionStorage.setItem(${sidebarScrollKey}, nav.scrollTop) } catch {}
  })
}`

const Toc = ({ items }: { items: TocItem[] }) => (
  <ul className="space-y-1.5">
    {items.map((item) => (
      <li key={item.id}>
        <a href={`#${item.id}`} className="text-sans-sm text-secondary hover:text-default block leading-tight">
          <Html html={item.title} />
        </a>
        {item.children.length > 0 && (
          <div className="mt-1.5 ml-3">
            <Toc items={item.children} />
          </div>
        )}
      </li>
    ))}
  </ul>
)

export function DocPage({
  sections,
  page,
  body,
  prev,
  next,
}: {
  sections: Section[]
  page: Page
  body: string
  prev?: Page
  next?: Page
}) {
  return (
    <Shell title={`${plain(page.title)} | ${site.title}`} root={rootFrom(page.out)}>
      <div className="flex">
        <nav
          id="sidebar"
          className="border-secondary 900:block sticky top-14 hidden h-[calc(100vh-3.5rem)] w-64 shrink-0 overflow-y-auto border-r px-6 py-8 [overflow-anchor:none]"
        >
          <Nav sections={sections} current={page} />
        </nav>
        <script>{sidebarScrollScript}</script>
        <main className="900:px-12 min-w-0 flex-1 px-6 py-10">
          <div className="mx-auto max-w-[760px]">
            <details className="900:hidden border-secondary mb-8 rounded border px-4 py-3">
              <summary className="text-mono-sm text-secondary">Menu</summary>
              <div className="mt-4">
                <Nav sections={sections} current={page} />
              </div>
            </details>
            <div className="text-mono-sm text-tertiary mb-2">{page.section}</div>
            {/* Only this part of the page goes in the search index */}
            <div data-pagefind-body="">
              <h1 className="text-sans-3xl text-raise mb-2">
                <Html html={page.title} />
              </h1>
              <a
                href={`${site.repo}/blob/main/${page.src}`}
                className="text-mono-xs text-tertiary hover:text-secondary mb-10 inline-block"
                data-pagefind-ignore=""
              >
                {page.src}
              </a>
              <div dangerouslySetInnerHTML={{ __html: body }} />
            </div>
            <div className="border-secondary mt-16 flex justify-between gap-4 border-t pt-6">
              {prev ? (
                <a href={relHref(page.out, prev.out)} className="group">
                  <div className="text-mono-xs text-tertiary">Previous</div>
                  <div className="text-sans-md text-secondary group-hover:text-default">
                    {plain(prev.title)}
                  </div>
                </a>
              ) : (
                <span />
              )}
              {next && (
                <a href={relHref(page.out, next.out)} className="group text-right">
                  <div className="text-mono-xs text-tertiary">Next</div>
                  <div className="text-sans-md text-secondary group-hover:text-default">
                    {plain(next.title)}
                  </div>
                </a>
              )}
            </div>
          </div>
        </main>
        {page.toc.length > 0 && (
          <aside className="1200:block sticky top-14 hidden h-[calc(100vh-3.5rem)] w-64 shrink-0 overflow-y-auto px-6 py-10">
            <div className="text-mono-xs text-tertiary mb-3">On this page</div>
            <Toc items={page.toc} />
          </aside>
        )}
      </div>
    </Shell>
  )
}

export function IndexPage({ sections }: { sections: Section[] }) {
  return (
    <Shell title={`${site.title} developer docs`} root=".">
      <main className="mx-auto max-w-[1100px] px-6 py-16">
        <h1 className="text-sans-4xl text-raise mb-4">{site.title} developer docs</h1>
        <p className="text-sans-xl text-secondary mb-14 max-w-[640px]">{site.description}</p>
        <div className="700:grid-cols-2 1000:grid-cols-3 grid grid-cols-1 gap-6">
          {sections.map((section) => (
            <section key={section.title} className="bg-raise border-secondary rounded-lg border p-6">
              <h2 className="text-sans-xl text-raise mb-1">{section.title}</h2>
              {section.description && (
                <p className="text-sans-md text-tertiary mb-4">{section.description}</p>
              )}
              <ul className="space-y-1.5">
                {section.pages.map((p) => (
                  <li key={p.out}>
                    <a href={p.out} className="text-sans-md text-accent-secondary hover:text-accent">
                      {plain(p.title)}
                    </a>
                  </li>
                ))}
              </ul>
            </section>
          ))}
        </div>
      </main>
    </Shell>
  )
}
