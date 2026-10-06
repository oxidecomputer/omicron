# Developer docs site

A static site built from the AsciiDoc and Markdown docs in this repo, deployed to
GitHub Pages by `.github/workflows/docs-site.yml` on every push to main that touches
docs.

## Adding a page

Add the file's path to a section in [`nav.ts`](nav.ts). Only listed files are
published. The page title comes from the document's own `= Title` (or `# Title` in
Markdown) unless the entry overrides it.

Links between docs work the same as on GitHub: `xref:other-doc.adoc[]` points at the
other page if it's on the site. Links to anything else in the repo (source files,
unlisted docs) go to the file on GitHub, and images are copied into the site.

## Building locally

Requires Node 24.

```
cd docs/site
npm install
npm run serve
```

That builds the site into `dist/` and serves it at http://localhost:1414. Pages
live at directory URLs, so opening the HTML from `file://` doesn't work. To build
without serving, run `npm run build`.

The build warns about broken links and Asciidoctor errors. To see which docs in the
repo aren't on the site, run `npm run unlisted`. The site uses system fonts locally
unless `docs/site/fonts` contains the Oxide font files (for example, a symlink to
`app/ui/assets/fonts` in a console checkout).

## How it works

`build.ts` passes the config in `nav.ts` to the builder in `lib/`, which has
nothing Omicron-specific in it so it can move to `@oxide/design-system` once
we're happy with it. `lib/render.tsx` renders AsciiDoc the same way the
RFD site and docs.oxide.computer do, with `@oxide/react-asciidoc` and the AsciiDoc
components and styles from `@oxide/design-system`. Markdown goes through
`markdown-exit`, with code blocks highlighted by shiki in the design system's theme.
`lib/links.ts` rewrites links between docs, and `lib/layout.tsx` is the page chrome.
Each doc's URL mirrors its path in the repo: `docs/how-to-run.adoc` is
published at `docs/how-to-run/`, and a README at its directory, so
`wicket/README.md` is at `wicket/`. Relative links and images in a doc are
rewritten to match. The builder
then runs [Pagefind](https://pagefind.app/) over the HTML in `dist/`, which writes
the search index and UI to `dist/pagefind/`, all loaded client-side, so search needs
no server. Last, Tailwind compiles `style.css`, which pulls in the shared styles
from `lib/site.css`, against the generated HTML.

## No client-side React

The pages are static. React only runs at build time to render HTML; the pages
aren't hydrated, so design system components render in their initial state, and
anything that depends on React state or effects needs to be redone in small
inline scripts in `lib/layout.tsx`. So far that's the mobile nav, the sidebar's
scroll position, and the outline's active item.

Hydrating would mean shipping about 60KB of React plus a client bundle and
serialized page data, in place of under 2KB of inline script, and adding a
second build step (Vite) to produce that bundle alongside the HTML. If we end up
wanting more interactive pieces, like a collapsible outline on small screens,
try plain HTML first and then hydrating just those regions, not whole pages.
