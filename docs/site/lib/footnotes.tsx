import { RenderInline, type DocumentBlock } from '@oxide/react-asciidoc'

/** Footnotes styled like the RFD site, with numbers linking back to the text. */
export function Footnotes({ document }: { document: DocumentBlock }) {
  if (document.footnotes.length === 0 || document.attributes.nofootnotes !== undefined) return null

  return (
    <section id="footnotes" className="border-secondary 800:mt-16 mt-12 border-t pt-4">
      <h2 className="text-mono-xs text-tertiary mb-4">Footnotes</h2>
      <ul className="space-y-3">
        {document.footnotes.map((footnote: DocumentBlock['footnotes'][number]) => (
          <li
            key={footnote.index}
            id={`_footnotedef_${footnote.index}`}
            className="flex items-baseline gap-3"
          >
            <a
              href={`#_footnoteref_${footnote.index}`}
              aria-label={`Back to reference ${footnote.index}`}
              className="text-mono-xs text-tertiary hover:text-accent w-6 shrink-0 text-right"
            >
              {footnote.index}
            </a>
            <p className="text-sans-md text-default min-w-0">
              <RenderInline nodes={footnote.textInlines} />
            </p>
          </li>
        ))}
      </ul>
    </section>
  )
}
