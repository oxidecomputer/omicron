/** Site config: the pages to publish and how to present them */
export type Site = {
  /** Project name, e.g. `Omicron` */
  title: string
  /** Shown after the title, e.g. `developer docs` */
  tagline: string
  description: string
  /** GitHub URL, e.g. `https://github.com/oxidecomputer/omicron` */
  repo: string
  /** Branch that source links point at */
  branch: string
  sections: {
    title: string
    description?: string
    /** Paths relative to the repo root, optionally with a title override */
    pages: (string | { path: string; title?: string })[]
  }[]
}

export type TocItem = { id: string; title: string; children: TocItem[] }

/** A rendered page */
export type Page = {
  /** Source path relative to the repo root, e.g. `docs/how-to-run.adoc` */
  src: string
  /** Output path relative to dist/, e.g. `docs/how-to-run.html` */
  out: string
  section: string
  title: string
  /** HTML body, not yet link-rewritten */
  body: string
  toc: TocItem[]
}

export type Section = { title: string; description?: string; pages: Page[] }
