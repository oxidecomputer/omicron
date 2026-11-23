// URL handling. Each doc gets a directory URL that mirrors its path in the
// repo, so links on the site look like the paths people know from GitHub.
// Relative links in a doc are written against its location in the repo, so
// this module resolves each one there, decides whether its target is a page on
// the site, an image to copy, or a file to link to on GitHub, and rewrites it
// relative to the page's URL.

import fs from 'node:fs'
import path from 'node:path'

import type { Page, Site, TocItem } from './types.ts'

/**
 * URL path of the doc at `src`, relative to the site root: `docs/foo.adoc` →
 * `docs/foo/`. A README is its directory's page, `wicket/README.md` →
 * `wicket/`, except the root README, which can't take the homepage's place and
 * gets `readme/`. Also maps the `.html` links Asciidoctor makes from xrefs.
 */
export function outPath(src: string) {
  const dir = path.posix.dirname(src)
  const name = path.posix.basename(src).replace(/\.(adoc|md|html)$/, '')
  if (name === 'README') return dir === '.' ? 'readme/' : `${dir}/`
  return dir === '.' ? `${name}/` : `${dir}/${name}/`
}

/**
 * Relative URL from the page at `from` to `to`, both relative to the site
 * root. A path ending in `/` is a page's directory URL; anything else is a file.
 */
export function relHref(from: string, to: string) {
  const base = from.endsWith('/') ? from : path.posix.dirname(from)
  const rel = path.posix.relative(base, to)
  if (!to.endsWith('/')) return rel
  return rel ? `${rel}/` : './'
}

/** URL of a file or directory in the repo on GitHub */
export const sourceUrl = (site: Site, repoPath: string, { dir = false } = {}) =>
  `${site.repo}/${dir ? 'tree' : 'blob'}/${site.branch}/${repoPath}`

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
  const idsByPage = new Map(
    pages.map((p) => [
      p,
      new Set([...p.body.matchAll(/<[a-z][^>]*?\sid="([^"]*)"/gi)].map((m) => m[1])),
    ]),
  )
  const flatToc = (items: TocItem[]): TocItem[] =>
    items.flatMap((item) => [item, ...flatToc(item.children)])
  const sectionTitlesByPage = new Map(
    pages.map((p) => [p, new Map(flatToc(p.toc).map((s) => [s.id, s.title]))]),
  )
  const assets = new Set<string>()

  // A section titled "External networking" has the ID `external-networking` on
  // GitHub and `_external_networking` on this site, because we render with
  // Asciidoctor's default idprefix and idseparator, which are both `_`.
  //
  // The docs link to sections both ways, sometimes in one file (see
  // how-to-run.adoc). <<external-networking>> works on GitHub but not here, and
  // <<_external_networking>> works here but not on GitHub. Setting idprefix and
  // idseparator to match GitHub would break the second kind here, so instead we
  // rewrite #external-networking to #_external_networking.
  //
  // A hash that's already an ID on the page is left alone (explicit anchors
  // like [[task-omdb-export]] keep their hyphens), and we only rewrite to an ID
  // that exists. Markdown pages are skipped: github-slugger already gives them
  // GitHub's IDs.
  function rewriteHash(hash: string, page: Page) {
    const ids = idsByPage.get(page)!
    if (!hash || !page.src.endsWith('.adoc') || ids.has(hash.slice(1))) return hash
    const id = `_${hash.slice(1).replaceAll('-', '_')}`
    return ids.has(id) ? `#${id}` : hash
  }

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

    // Asciidoctor can't resolve a GitHub-style <<some-heading>>, so with no
    // label it renders the ID in brackets. Use the section's title instead.
    html = html.replace(/<a href="#([^"]*)">\[\1\]<\/a>/g, (match, id: string) => {
      const hash = rewriteHash(`#${id}`, page)
      const title = sectionTitlesByPage.get(page)!.get(hash.slice(1))
      return hash !== `#${id}` && title ? `<a href="${hash}">${title}</a>` : match
    })

    // Only in tags: code blocks can contain href="..." as text, and shiki doesn't
    // escape the quotes
    const attrRe = /(<[a-z][^>]*?\s)(href|src)="([^"]*)"/gi
    return html.replace(attrRe, (match, start: string, attr: string, url: string) => {
      if (attr === 'href' && url.startsWith('#')) {
        return `${start}${attr}="${rewriteHash(url, page)}"`
      }
      const resolved = resolve(page, url)
      if (!resolved) return match
      const { target, hash, linked } = resolved

      if (linked) {
        return `${start}${attr}="${relHref(page.out, linked.out)}${rewriteHash(hash, linked)}"`
      }

      // Asciidoctor turns xref:foo.adoc[] into foo.html. If foo.adoc exists but
      // isn't on the site, link to the source on GitHub instead.
      const srcTarget =
        target.endsWith('.html') && !fs.existsSync(path.join(repoRoot, target))
          ? target.replace(/\.html$/, '.adoc')
          : target
      const stat = fs.statSync(path.join(repoRoot, srcTarget), { throwIfNoEntry: false })
      if (!stat) {
        console.warn(`${page.src}: broken link ${url}`)
        // A GitHub page URL can't render as an image anyway
        if (attr === 'src') return match
      } else if (attr === 'src') {
        assets.add(srcTarget)
        return `${start}${attr}="${relHref(page.out, srcTarget)}"`
      }
      return `${start}${attr}="${sourceUrl(site, srcTarget, { dir: stat?.isDirectory() })}${hash}"`
    })
  }

  return { rewrite, assets }
}
