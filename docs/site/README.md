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
npm run build
open dist/index.html
```

Search doesn't work from `file://`. To try it, run `npm run serve`, which builds the
site and serves it at http://localhost:1414.

The build warns about broken links and Asciidoctor errors. To see which docs in the
repo aren't on the site, run `npm run unlisted`. The site uses system fonts locally unless `docs/site/fonts`
contains the Oxide font files (for example, a symlink to `app/ui/assets/fonts` in a
console checkout).

## How it works

`build.ts` passes the config in `nav.ts` to the builder in `lib/`, which has
nothing Omicron-specific in it. `lib/render.tsx` renders AsciiDoc the same way the
RFD site and docs.oxide.computer do, with `@oxide/react-asciidoc` and the AsciiDoc
components and styles from `@oxide/design-system`. Markdown goes through
`markdown-exit`, with code blocks highlighted by shiki in the design system's theme.
`lib/links.ts` rewrites links between docs, and `lib/layout.tsx` is the page chrome.
The output mirrors each doc's path in the repo, so `docs/how-to-run.adoc`
becomes `dist/docs/how-to-run.html` and relative links keep working. Tailwind
compiles `style.css` against the generated HTML. [Pagefind](https://pagefind.app/)
then indexes the HTML in `dist/` and writes the search index and UI to
`dist/pagefind/`, all loaded client-side, so search needs no server.
