import { DesktopOutline } from '@oxide/design-system/asciidoc'
import { MenuClose12Icon, MenuOpen12Icon } from '@oxide/design-system/icons/react'
import type { DocumentSection } from '@oxide/react-asciidoc'
import type { ReactNode } from 'react'

import { relHref, sourceUrl } from './links.ts'
import type { Page, Section, Site, TocItem } from './types.ts'

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

/** Relative path from the page at `from` to the site root, e.g. `../..` */
const rootFrom = (from: string) => relHref(from, '.')

/** The mark from GitHub's Octicons, which the design system doesn't have */
const GitHubIcon = () => (
  <svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor" aria-hidden="true">
    <path d="M8 0c4.42 0 8 3.58 8 8a8.013 8.013 0 0 1-5.45 7.59c-.4.08-.55-.17-.55-.38 0-.27.01-1.13.01-2.2 0-.75-.25-1.23-.54-1.48 1.78-.2 3.65-.88 3.65-3.95 0-.88-.31-1.59-.82-2.15.08-.2.36-1.02-.08-2.12 0 0-.67-.22-2.2.82-.64-.18-1.32-.27-2-.27-.68 0-1.36.09-2 .27-1.53-1.03-2.2-.82-2.2-.82-.44 1.1-.16 1.92-.08 2.12-.51.56-.82 1.28-.82 2.15 0 3.06 1.86 3.75 3.64 3.95-.23.2-.44.55-.51 1.07-.46.21-1.61.55-2.33-.66-.15-.24-.6-.83-1.23-.82-.67.01-.27.38.01.53.34.19.73.9.82 1.13.16.45.68 1.31 2.69.94 0 .67.01 1.3.01 1.49 0 .21-.15.45-.55.38A7.995 7.995 0 0 1 0 8c0-4.42 3.58-8 8-8Z" />
  </svg>
)

/** When the mobile nav opens, center the current page's link in it */
const mobileNavScript = `{
  const nav = document.getElementById('mobile-nav')
  nav.addEventListener('toggle', (e) => {
    if (e.newState !== 'open') return
    const l = nav.querySelector('[aria-current=page]')
    nav.scrollTop = l.offsetTop - (nav.clientHeight - l.offsetHeight) / 2
  })
}`

function Shell({
  site,
  title,
  root,
  menu,
  children,
}: {
  site: Site
  title: string
  root: string
  /** Shown in a drawer from the header's menu button on small screens */
  menu?: ReactNode
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
        <header className="bg-default border-secondary 600:px-6 sticky top-0 z-10 flex h-14 items-center gap-3 border-b px-4">
          <a href={`${root}/`} className="text-sans-xl text-raise">
            {site.title}
          </a>
          <span className="text-mono-sm text-tertiary 600:inline hidden">{site.tagline}</span>
          <div className="ml-auto flex items-center gap-2">
            <pagefind-modal-trigger placeholder="Search" />
            <a
              href={site.repo}
              aria-label="GitHub"
              className="text-secondary hover:text-default border-default hover:bg-raise flex h-8 w-8 items-center justify-center rounded-lg border"
            >
              <GitHubIcon />
            </a>
            {menu && (
              <button
                popoverTarget="mobile-nav"
                aria-label="Menu"
                className="900:hidden border-default hover:bg-raise text-secondary hover:text-default flex h-8 w-8 items-center justify-center rounded-lg border"
              >
                <MenuOpen12Icon className="menu-icon-open" />
                <MenuClose12Icon className="menu-icon-close" />
              </button>
            )}
          </div>
        </header>
        {menu && (
          <>
            {/* A popover, so the browser handles opening, closing on Escape or a
                click outside, and focus. Positioned in site.css */}
            <nav
              id="mobile-nav"
              popover=""
              className="bg-default border-secondary text-default overflow-y-auto overscroll-contain border-0 border-r px-4 py-6"
            >
              {menu}
            </nav>
            <script>{mobileNavScript}</script>
          </>
        )}
        <pagefind-modal />
        {children}
      </body>
    </html>
  )
}

// Sidebar link rows styled like the guides sidebar on docs.oxide.computer. The
// row padding hangs outside the parent so the text lines up with what's around it.
const linkRow = (current: boolean) =>
  `text-sans-md block rounded-md px-2 py-0.75 ${
    current ? 'bg-accent text-accent hover:bg-accent-hover' : 'text-secondary hover:bg-hover'
  }`

function Nav({ sections, current }: { sections: Section[]; current: Page }) {
  return (
    <div className="-mx-2 space-y-6">
      {sections.map((section) => (
        <div key={section.title}>
          <div className="text-sans-md text-raise mb-1 ml-2">{section.title}</div>
          <ul>
            {section.pages.map((p) => (
              <li key={p.out} className="py-px">
                <a
                  href={relHref(current.out, p.out)}
                  aria-current={p === current ? 'page' : undefined}
                  className={linkRow(p === current)}
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
const sidebarScrollScript = (site: Site) => {
  const key = JSON.stringify(`sidebar-scroll:${site.repo}`)
  return `{
  const nav = document.getElementById('sidebar')
  try { nav.scrollTop = Number(sessionStorage.getItem(${key})) } catch {}
  const n = nav.getBoundingClientRect()
  const l = nav.querySelector('[aria-current=page]').getBoundingClientRect()
  if (l.top < n.top || l.bottom > n.bottom) nav.scrollTop += l.top - n.top - (n.height - l.height) / 2
  addEventListener('pagehide', () => {
    try { sessionStorage.setItem(${key}, nav.scrollTop) } catch {}
  })
}`
}

/** The shape `DesktopOutline` takes, which shows levels 1 and 2 */
const toOutline = (items: TocItem[], level = 1): DocumentSection[] =>
  items.map((item) => ({
    id: item.id,
    title: item.title,
    level,
    num: '',
    numbered: false,
    hasCaption: false,
    sections: toOutline(item.children, level + 1),
  }))

/**
 * `DesktopOutline` expects React state to say which item is active, but these
 * pages aren't hydrated (see the README). The page renders with the first item
 * active, so this reads the active and inactive class lists off the links
 * (which also keeps them in the HTML for Tailwind to find) and swaps them as
 * you scroll. The active section is the last one whose heading has scrolled
 * past the top fifth of the viewport, or the last one when you hit the bottom
 * of the page. The outline renders deeper levels hidden, so skip those, leaving
 * their parent active.
 */
const outlineScript = `{
  const links = [...document.querySelectorAll('#outline .toc li:not(.hidden) a')]
  const on = links[0].className
  const off = links.find((l) => l.className !== on)?.className
  const heads = links.map((l) => document.getElementById(l.getAttribute('href').slice(1)))
  let current = links[0]
  const update = () => {
    const atBottom = innerHeight + scrollY >= document.documentElement.scrollHeight - 1
    let i = 0
    heads.forEach((h, j) => { if (h && h.getBoundingClientRect().top < innerHeight / 5) i = j })
    const next = links[atBottom ? links.length - 1 : i]
    if (next === current) return
    current.className = off
    next.className = on
    current = next
  }
  let queued = false
  if (off) addEventListener('scroll', () => {
    if (queued) return
    queued = true
    requestAnimationFrame(() => { queued = false; update() })
  }, { passive: true })
  if (off) update()
}`

export function DocPage({
  site,
  sections,
  page,
  body,
  prev,
  next,
}: {
  site: Site
  sections: Section[]
  page: Page
  body: string
  prev?: Page
  next?: Page
}) {
  return (
    <Shell
      site={site}
      title={`${plain(page.title)} | ${site.title}`}
      root={rootFrom(page.out)}
      menu={<Nav sections={sections} current={page} />}
    >
      <div className="flex">
        <nav
          id="sidebar"
          className="border-secondary 900:block sticky top-14 hidden h-[calc(100vh-3.5rem)] w-64 shrink-0 overflow-y-auto border-r px-6 py-8 [overflow-anchor:none]"
        >
          <Nav sections={sections} current={page} />
        </nav>
        <script>{sidebarScrollScript(site)}</script>
        <main className="600:px-6 900:px-12 min-w-0 flex-1 px-4 py-10">
          <div className="mx-auto max-w-[760px]">
            <div className="text-mono-sm text-tertiary mb-2">{page.section}</div>
            {/* Only this part of the page goes in the search index */}
            <div data-pagefind-body="" className="wrap-break-word">
              <h1 className="text-sans-2xl 600:text-sans-3xl text-raise mb-2">
                <Html html={page.title} />
              </h1>
              <a
                href={sourceUrl(site, page.src)}
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
          <aside
            id="outline"
            className="1200:block sticky top-14 hidden h-[calc(100vh-3.5rem)] w-64 shrink-0 overflow-y-auto px-6 py-10"
          >
            <div className="text-mono-xs text-tertiary mb-3">On this page</div>
            <DesktopOutline toc={toOutline(page.toc)} activeItem={page.toc[0].id} />
            <script>{outlineScript}</script>
          </aside>
        )}
      </div>
    </Shell>
  )
}

export function IndexPage({ site, sections }: { site: Site; sections: Section[] }) {
  return (
    <Shell site={site} title={`${site.title} ${site.tagline}`} root=".">
      <main className="600:px-6 700:py-16 mx-auto max-w-[1100px] px-4 py-8">
        <h1 className="text-sans-3xl 700:text-sans-4xl 700:mb-10 mb-6 text-balance">
          <span className="text-accent">{site.title}</span>{' '}
          <span className="text-raise">{site.tagline}</span>
        </h1>
        <div className="700:grid-cols-2 1000:grid-cols-3 700:gap-6 grid grid-cols-1 gap-3">
          {sections.map((section) => (
            <section
              key={section.title}
              className="bg-raise border-secondary 700:p-6 rounded-lg border p-5"
            >
              <h2 className="text-sans-lg 700:text-sans-xl text-raise 700:mb-4 700:gap-3 mb-3 flex items-center gap-2.5">
                {section.icon && (
                  <span className="text-accent bg-accent 700:p-1.5 inline-flex rounded-md p-1">
                    <section.icon />
                  </span>
                )}
                {section.title}
              </h2>
              <ul className="-mx-2">
                {section.pages.map((p) => (
                  <li key={p.out} className="py-px">
                    <a
                      href={p.out}
                      className="text-sans-md text-default hover:bg-hover hover:text-raise block rounded-md px-2 py-0.75"
                    >
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
