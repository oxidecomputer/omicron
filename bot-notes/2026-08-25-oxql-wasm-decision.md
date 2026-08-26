# OxQL client parser: Wasm decision and regex-completer improvements

Date: 2026-08-25

> Agent-written note, originally in the console repo's unversioned notes dir.
> Paths like `app/components/...` refer to the console repo; the regex-completer
> improvements described below live in a console branch, not here.

## Question

Should the console adopt the Rust/Wasm OxQL parser pipeline spiked in Omicron,
or is it overkill? Companion notes:

- [Client parser options](2026-08-25-oxql-client-parser-options.md) (written in
  console)
- [Lezer spike](2026-08-25-oxql-lezer-spike.md) (written in console; spike code
  is in the console repo, but its 15 probe cases are checked in here as
  `oximeter/oxql/test-data/completion-corpus.json`)
- Omicron extraction plan and results:
  [extraction plan](2026-08-25-oxql-parser-wasm-extraction-plan.md),
  implemented in the two commits on this branch (parse pipeline, then
  completion context) on top of omicron main as of 2026-08-25.

## Decision: not now

The Wasm pipeline is proven feasible but the product case isn't there. Keep
and improve the regex completer; promote the Lezer spike if we want
structurally correct completion; revisit Wasm only if OxQL editing becomes a
product goal deserving live semantic diagnostics and table-shape-aware
completion.

Reasons:

- **The completion win is small.** The Rust `completion_context` API improves
  6 of the 15 Lezer probe cases over the current regexes: no identifier
  suggestions at literal positions, none after complete literals, immunity to
  `|`/`;` inside strings, no completion of a finished outer `join`, and fields
  scoped to the innermost subquery. Most of those are cheaply achievable in
  the regexes (done below) or fully covered by Lezer at ~3 KB gzipped with no
  cross-repo machinery.
- **The sync problem it solves barely exists.** Nine commits ever to
  `grammar.rs`, zero language changes since 2024-08-19. A shared corpus plus a
  grammar diff check on `OMICRON_VERSION` bumps covers drift essentially for
  free.
- **The fragile part is exactly the part completion needs.** The strict PEG
  requires a prefix-repair layer for incomplete input, and the spike already
  needed a bespoke recovery pass for one probe case (incomplete inner filter
  with a complete outer join). That tax is permanent.
- **The real payoff hasn't been built.** Live semantic diagnostics and
  shape-aware completion via `analyze(query, schemas)` are the features that
  would justify the boundary, and they carry the largest hidden cost: making
  the extracted parser/AST canonical in Omicron (syntax nodes and execution
  methods are interleaved today), plus schema staleness/permission caveats.
- **Costs are structural, not incremental:** ~192 KB Wasm + 9 KB glue,
  loading/failure behavior, a versioned artifact pipeline between repos, and a
  Wasm CI toolchain neither repo has.

The omicron note's "suggested decision bar" section is the checklist to apply
if this is revisited: a console prototype must beat the regex implementation
on live diagnostics, semantic completion, an adversarial cursor corpus,
measured load/keystroke latency, and a concrete versioning workflow.

## Interesting implementation details from the spikes

Worth remembering if the Wasm route is picked up later:

- **Repair-the-prefix strategy.** `oximeter/oxql/src/completion.rs` makes a
  strict PEG answer cursor queries by appending candidate suffixes to the
  prefix before the cursor (`datum == 0`, `], mean`, `:placeholder`,
  keyword completions like `filter datum == 0`, etc.), plus auto-closing
  unbalanced braces, and taking the first repair that parses. The repair that
  succeeds determines the `CompletionSite`. It's clever but enumerative — each
  new grammar construct needs matching repairs.
- **Width-preserving clause recovery.** For an incomplete _earlier_ clause
  (e.g. `{ get a | filter sl; get b } | join` with more text after), it
  overwrites the dangling `filter <ident>` in place with `last 1` padded with
  spaces, preserving all byte offsets so spans stay valid. This is the
  special-case pass that signals the approach won't stay simple.
- **Spans in the AST.** Query and timeseries nodes record source spans, which
  lets `innermost_query_at(cursor)` select scope without a second parser —
  the same trick the Lezer tree gives for free.
- **UTF-16 boundary conversion.** JS string offsets are UTF-16 code units;
  the Wasm wrapper converts cursor and replacement spans between UTF-16 and
  UTF-8 byte offsets and rejects non-boundary cursors. Any client parser API
  crossing the JS boundary needs this; there's a Node test for it.
- **Hakari exclusion is design, not workaround.** Wasm-capable crates must be
  excluded from `omicron-workspace-hack` traversal or the dependency graph
  breaks for `wasm32-unknown-unknown`.
- **Sizes.** Release artifact: 191,640 bytes Wasm + 9,368 bytes JS glue
  (parse + completion). The Lezer parser for comparison: 6.7 KB unminified,
  3.1 KB gzipped.
- **`interpolate` is a trap.** The grammar accepts `align interpolate(1m)` but
  planning rejects it as unimplemented, so it should _not_ be added to
  completions despite being a "grammar parity" gap — completing it would steer
  users into guaranteed server errors. Revisit when omicron implements it.

## Regex completer improvements (prototyped in this rev)

Extended `app/components/oxql-autocomplete.ts` to cover the correctness gaps
that don't need a parser, replacing the `||`-blanking hack with a single
string-aware scan of the document before the cursor:

1. **Quote awareness.** `|`, `;`, `{`, `}` inside string literals no longer
   act as clause/scope boundaries, and no completions are offered while the
   cursor is inside an unterminated string.
2. **Literal positions.** After a comparison operator in a filter, fields are
   no longer offered; only `@now()` is (the one completable literal). After a
   complete quoted literal, nothing is offered.
3. **Innermost-subquery scope.** Field completions now come only from `get`s
   in the innermost query branch containing the cursor (brace/semicolon
   scoping), instead of every `get` in the document.
4. **`datum`** added to filter identifier extras.
5. **`get` position rule.** `get` is offered only as the first operation of a
   query branch and excluded from the operations offered after a pipe (the
   grammar requires every query to start with `get` and forbids a later one).
   This reused the scanner's existing `clauseStart === scopeStart` distinction
   with no new state.
6. **Boolean literals.** `true`/`false` offered alongside `@now()` at filter
   literal positions.
7. **Positional filter completion.** An edge-case hunt found the filter branch
   offered fields at operator and post-literal positions (`filter x == 5 `,
   `filter timestamp > @now()`, `filter timestamp > @now() - `, `filter x `,
   after `)`), and offered `@now()` at identifier positions where the grammar
   (`comparison_atom = ident op literal`) forbids it. Fixed by inverting to a
   positive rule that is simpler than the tail-matching it replaced:
   identifiers only at the start of a boolean operand (after `filter`, `&&`,
   `||`, `^`, `(`, `!`), literals only right after a comparison operator,
   nothing elsewhere. Also blanked string literals before scanning the scope
   for `get`s so quoted text can't inject a phantom timeseries.

Known remaining quirk: right after `}` (before the user types `|`), table
operations are offered though only a pipe is legal there. Fixing it needs the
scanner to track boundary kind — new state, i.e. the Lezer tripwire — and the
suggestions are at least never `get`.

Not done, deliberately: `interpolate` (see above), suppressing table-op
completion after a finished `join` (harmless with prefix filtering, not worth
a special case), and distribution-specific identifiers (need schema-type
awareness to offer non-noisily).

## What the prototype showed

The prototype strengthens the keep-the-regexes position: four of the six
correctness wins the Rust API demonstrated landed in ~70 lines with no new
dependencies, including the two headline Lezer probe results (string-immune
clause splitting, innermost-subquery scope). The gap a client parser would
close is much narrower than the probe framing suggested.

Caveats:

- The completer is no longer purely regexes: the fix required a hand-rolled
  quote-aware scanner with a brace stack feeding clause-tail regexes. It won
  by becoming slightly parser-shaped. Future demands (parens/negation scope
  in filters, cursor positions with text after them) add bespoke state rather
  than falling out of a grammar.
- Lezer's remaining case is the tree-only features: native highlighting to
  replace Shiki, live syntax diagnostics, folding, arbitrary-cursor behavior.
  Without those on the roadmap it is hard to justify now.

Tripwire for revisiting Lezer: if the scanner needs another state variable or
the filter literal-position regex grows another alternative, port to the
existing 71-line spike grammar instead — it replaces exactly that code. Wasm
still waits on wanting semantic features (type-aware literals, shape-aware
operation completion) that neither regexes nor Lezer can provide.
