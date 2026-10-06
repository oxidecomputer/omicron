// URL handling. The site mirrors each doc's path in the repo, so a relative
// link that works on GitHub resolves the same way here. This module decides
// whether its target is a page on the site, an image to copy, or a file to
// link to on GitHub.

import fs from 'node:fs'
import path from 'node:path'

import type { Page, Site } from './types.ts'

/** Output path of the doc at `src`, e.g. `docs/foo.adoc` → `docs/foo.html` */
export const outPath = (src: string) => src.replace(/\.(adoc|md)$/, '.html')

/** Relative URL from the page at `from` to the file at `to`, both relative to dist/ */
export const relHref = (from: string, to: string) =>
  path.posix.relative(path.posix.dirname(from), to)

/**
 * Rewrites links in page bodies. Collects the images they reference in
 * `assets`, as paths relative to the repo root, for the build to copy.
 */
export function createLinkRewriter({
  site,
  repoRoot,
  pages,
}: {
  site: Site
  repoRoot: string
  pages: Page[]
}) {
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
   * Point links at the right place. A link to another page on the site becomes
   * a relative link to its HTML; an image is copied into dist/ at the same
   * relative path; anything else in the repo links to the file on GitHub.
   */
  function rewrite(html: string, page: Page) {
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

      if (linked) return `${attr}="${relHref(page.out, linked.out)}${hash}"`

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

  return { rewrite, assets }
}
