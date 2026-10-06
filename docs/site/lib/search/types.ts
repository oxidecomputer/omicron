// The interface between the site builder and a search engine. A provider has
// a build side, which indexes the site's pages and writes whatever files its
// client needs, and a client module that the search modal (lib/search.ts)
// loads in the browser the first time it opens. The search eval
// (search-eval/) loads the same client in Node.

/** A page to index */
export type SearchPage = {
  /** URL path relative to the site root, e.g. `docs/how-to-run/` */
  url: string
  /** HTML, since titles can have markup like `<code>` */
  title: string
  /** Rendered body, with links rewritten */
  html: string
}

export type SearchProvider = {
  /** Index `pages` and write the index to `dir`, which the build creates */
  build(pages: SearchPage[], dir: string): void | Promise<void>
  /**
   * Path to the client module, a TypeScript file that `stripTypeScriptTypes`
   * can handle, whose `load` export is a `LoadEngine`. The build copies it to
   * `dir` as engine.js.
   */
  client: string
}

/** A section to show under a page. `excerpt` is HTML with matches in `<mark>`. */
export type SectionResult = { title: string; url: string; excerpt: string }

/**
 * A page to show, with up to 3 of its sections. URLs are relative to the site
 * root, and `excerpt`s are HTML: the client escapes the page text in them.
 */
export type Result = { url: string; title: string; excerpt: string; sections: SectionResult[] }

export type Engine = {
  /** Results in the order to show them */
  search(query: string): Result[] | Promise<Result[]>
}

/** Load the engine whose index the build wrote to `dir` */
export type LoadEngine = (dir: URL) => Promise<Engine>
