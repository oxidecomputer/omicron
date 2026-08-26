# Client-side OxQL parser options

Date: 2026-08-25

> Agent-written note, originally in the console repo's unversioned notes dir.
> Paths like `app/components/...` refer to the console repo;
> `../omicron/...` paths refer to this repo.

## Goal

Assess whether `oxqlCompletionSource` should be replaced or supported by a complete client-side OxQL parser. Compare:

1. Recreating the authoritative Omicron parser in TypeScript, including the likely maintenance cost based on parser churn.
2. Compiling the Rust parser to WebAssembly and using it directly in the console.

The result should recommend an approach and identify the implementation scope, integration shape, risks, and useful incremental steps.

## Current console implementation

`app/components/oxql-autocomplete.ts` is 144 lines, including completion metadata. Its structural model is:

- Find the current word with `/[@\w:]*/`.
- Treat `|`, `{`, and `;` as clause boundaries, after blanking `||` so it is not mistaken for two pipes.
- Match the current clause against regexes for `get`, `align`, `group_by`, and `filter`.
- Find every `get` in the entire document with `/\bget\s+([\w:]+)/g`, then offer the union of fields from those schemas.

The completer covers all seven table-operation keywords, `mean_within`, the `mean` and `sum` reducers, and three filter extras (`timestamp`, `start_time`, and `@now()`). The authoritative grammar also accepts `interpolate`; filter/planning code recognizes `datum` and distribution-specific identifiers. More generally, the completer does not model filter operators, literal positions, parentheses, nested query scope, or pipes/braces/semicolons inside strings.

The schema lookup remains necessary with any parser. The OxQL grammar parses identifiers but does not know which schemas or fields exist, so syntax context and API-backed semantic completions are separate layers.

`OxqlEditor` currently uses Shiki/TextMate for whole-document highlighting and only displays a server parse diagnostic after submission. A client parser could additionally provide live syntax diagnostics and structural highlighting, but those are optional follow-ons.

## Authoritative parser architecture

The parser is a real Rust PEG in `../omicron/oximeter/db/src/oxql/ast/grammar.rs`:

- `peg::parser!` starts at line 7.
- The grammar and its helpers occupy lines 1–669; tests occupy lines 670–1413.
- There are 71 rules, including public entry points for individual literal/operation forms and the complete `query` rule.
- The query grammar covers nested subqueries, all seven table operations, filter precedence and negation, comparisons, reducers, two alignment methods, and typed literals (boolean, duration, decimal/hex integer, float, UUID, IP, string, and timestamp).

It is not a standalone grammar file. Rules contain Rust return types and semantic actions that:

- construct the Rust AST;
- parse and range-check numeric values;
- parse UUIDs, IP addresses, durations, and timestamps;
- unescape strings;
- validate timeseries names; and
- enforce that every query/subquery starts with `get` and has no later `get`.

The parsing types are embedded in AST modules that also implement query execution and planning behavior. The current AST module set is about 4,405 lines excluding the grammar, but most of that is table/filter execution rather than syntax representation. Important coupling includes `chrono`, `uuid`, `oximeter::TimeseriesName`, `oxql-types`, and database-specific filtering/table operations.

`oximeter-db` is not a viable browser-Wasm compilation unit as-is. Even with `--no-default-features --features oxql`, `cargo tree` shows dozens of unconditional direct dependencies, including Dropshot, ClickHouse clients/types, reqwest, Tokio, DTrace support, terminal libraries, and Omicron workspace crates. A small parser crate must be extracted first.

The public `oxql::query::Query::new` also trims input, enforces a 4,096-byte limit, calls the PEG parser, formats its error, and resolves timestamp expressions to a query end time. The PEG itself is primarily syntactic; schema/type validity and operation preconditions are checked later by planning/execution.

## Parser churn

Local Omicron history from 2024-03-29 through 2026-08-25 shows nine commits touching `grammar.rs` after/including introduction:

| Date | Change | Language impact |
| --- | --- | --- |
| 2024-03-29 | Initial OxQL implementation | Initial grammar |
| 2024-04-11 | Remove flaky test | None |
| 2024-04-12 | Add `first`/`last` | New syntax (42 grammar lines) |
| 2024-04-16 | Clippy cleanup | None |
| 2024-06-21 | Cargo docs fix | None |
| 2024-08-16 | Case-insensitive UUID parsing | Accepted-literal fix (21 changed lines) |
| 2024-08-19 | Hex integer literals | New literal form (47 changed lines) |
| 2025-03-03 | Rust 2024 formatting/style | None |
| 2026-07-06 | Copyright cleanup | None |

Current line attribution reinforces this: 1,268 of 1,413 grammar/test lines still originate in the initial implementation. The only attributed substantive additions are limits (37 surviving lines), uppercase UUID support (15), and hex integers/tests (39). There have been no accepted-language changes since 2024-08-19.

AST files have seen more activity from planner/execution work, but that is mostly irrelevant to a syntax-only client parser. For duplication cost, grammar history is the better signal.

## Option A: TypeScript implementation

There are two reasonable forms.

### A1. A TypeScript PEG (closest port)

Port the Rust PEG to a JS/TS PEG generator such as Peggy. The productions translate mechanically because both are PEGs. Rust-specific work remains:

- Define a small TS AST or return raw/token values.
- Translate semantic actions and validations. Use `bigint` or retain raw strings for integer literals to avoid JS number precision differences.
- Decide whether timestamps should be validated only or evaluated. For editor use, preserving the source expression is safer than reproducing the server's time-dependent `@now()` value.
- Implement/retain exact string escaping, UUID/IP validation, duration overflow rules, and the query-start invariant if strict parity is required.
- Port the accepted/rejected query corpus from the Rust tests.

This is likely a few hundred lines of grammar/actions plus tests, not a port of the 4,405 lines of execution-heavy AST modules. The grammar's low churn makes manual synchronization credible.

Limitations for autocomplete: a strict PEG normally rejects the unfinished document users are editing. Completion needs one of:

- a grammar-level cursor/hole token that returns the enclosing syntactic context;
- tolerant/prefix entry rules; or
- expected-token errors plus a partial scanner/AST for dynamic schema fields.

Without one of those, a complete PEG improves validation but does not replace all current completion heuristics.

### A2. A Lezer grammar (best CodeMirror fit)

Write the same syntax as a Lezer grammar and use its error-tolerant incremental tree in CodeMirror. This gives reliable cursor context in incomplete queries, nested scope, and a path to native syntax highlighting and live linting. `@codemirror/language` and Lezer runtime packages are already present; the Lezer generator would become a direct development dependency.

The cost is a less mechanical port: Lezer is LR-based, semantic validation/actions live outside the grammar, and exact parity must be proven through fixtures. For an editor, separating concrete syntax from semantic validation is useful: the tree can remain available while the query is incomplete.

### Keeping the copy synchronized

Whichever TS parser is chosen:

- Record the Omicron grammar revision used for the port.
- Port the Rust positive/negative parser cases into a data-driven corpus rather than rewriting each as bespoke TS assertions.
- On `OMICRON_VERSION` bumps, compare `grammar.rs` between old and new pins and make a grammar change an explicit review item. The console's pin is currently `6db4c7e` (2026-08-17), so parser behavior can be tied to the same backend revision.
- Treat client diagnostics as advisory; the pinned server remains authoritative on submit.

A shared neutral grammar could generate both implementations, but the embedded semantic actions mean adopting one would require changing Omicron's parser architecture. Given the observed churn, that machinery is harder to justify than maintaining the small TS syntax grammar and conformance corpus.

## Option B: Rust/Wasm implementation

The useful version of this option is not “compile `oximeter-db`.” It is:

1. Extract an `oxql-parser` (or similarly scoped) Rust crate containing the grammar, a lean syntax AST, parse error representation, and only browser-compatible dependencies.
2. Convert the parser AST recursively into the existing `oximeter-db` AST. This lets the large execution-oriented implementations stay unchanged. A more invasive follow-up could make the parser AST canonical and turn the database methods into extension traits, but that is not required for an initial extraction.
3. Add a small `wasm-bindgen` wrapper and produce ESM glue plus a `.wasm` asset.
4. Establish artifact distribution/versioning for console. There is no current Wasm build/publish pipeline in either repo. Plausible choices are an npm package or checked-in generated artifact, both tied to an Omicron/parser version.
5. Load the module before or shortly after editor initialization and reconfigure the CodeMirror extension once ready.

Advantages:

- Completed-query acceptance and parse errors come from the same code as the server.
- Future grammar changes automatically affect native and Wasm builds once the artifact is updated.
- A standalone parser crate is reusable by other Rust/native consumers.

Costs and unresolved pieces:

- The initial Omicron refactor is larger than copying the grammar to TS, but does not need to be proportional to the 4,405 execution-heavy AST lines. With a lean parser AST and conversion layer, it is a contained crate extraction plus tests and Wasm bindings.
- Wasm artifact production, publication, pinning, and review become cross-repo release concerns. The console currently has no Rust/Wasm CI toolchain.
- `chrono`, `uuid`, serialization, and `wasm-bindgen` need a size/compatibility spike after extraction.
- A strict Rust PEG still does not provide an error-tolerant syntax tree. Sharing the completed-query parser does not by itself solve cursor context for incomplete input. The Rust API would need a completion-aware/prefix parser or the TS side would retain structural heuristics.
- Serializing a full AST across the Wasm boundary is unnecessary if the only API is validation/context. A deliberately small API (`validate`, parse diagnostic, referenced timeseries, completion context) would reduce coupling.

Wasm is credible for this editor if exact parser reuse is valuable enough to establish the artifact pipeline. The extraction itself is tractable; artifact distribution and partial-input analysis are the larger design questions.

## CodeMirror integration model

CodeMirror has three relevant levels of integration.

### 1. Lite providers

CodeMirror's normal extension APIs already cover a small language-aware editor:

- `CompletionSource` supplies synchronous or asynchronous completions.
- `linter()` runs after the editor becomes idle—750 ms by default—and accepts
  asynchronous diagnostics.
- `hoverTooltip()` handles hover information.

These work without a CodeMirror language grammar or LSP. This is the intended
lightweight integration model. For OxQL, the shape would be:

```text
CodeMirror completion source ──→ regexes or Wasm completion API
CodeMirror lint source ────────→ Wasm analyze(query, schemas)
```

CodeMirror supplies scheduling, stale-result handling, completion UI,
diagnostic rendering, gutters, and tooltips. Console only has to translate the
Rust results into CodeMirror objects.

References:

- [CodeMirror autocompletion example](https://codemirror.net/examples/autocompletion/)
- [CodeMirror lint example](https://codemirror.net/examples/lint/)

### 2. CodeMirror language package

Full CodeMirror language support normally uses a Lezer parser. That produces
the incremental tree used for syntax-aware completion, highlighting,
indentation, and folding. CodeMirror also offers `StreamLanguage`, a lighter
stateful tokenizer mainly suitable for highlighting.

The Rust PEG does not naturally provide CodeMirror's Lezer tree. Adapting it to
that interface would be substantial work. A stream tokenizer would introduce
another client-side approximation.

References:

- [CodeMirror `Language` API](https://codemirror.net/docs/ref/#language.Language)
- [CodeMirror language-package guide](https://codemirror.net/examples/lang-package/)

### 3. LSP

CodeMirror now has an official
[`@codemirror/lsp-client`](https://github.com/codemirror/lsp-client). It supports
completions, diagnostics, hover, signature help, navigation, rename, and
formatting. It can connect over WebSocket or to a client-side Wasm server
through a small transport abstraction.

That package handles LSP synchronization, request concurrency, position
mapping, and UI adapters. OxQL would still need an LSP server implementing
document lifecycle, JSON-RPC messages, capabilities, and each language feature.
LSP also does not supply the parser or semantic intelligence.

Reference:

- [Official `@codemirror/lsp-client` announcement](https://discuss.codemirror.net/t/codemirror-lsp-client/9309)

### Recommendation for OxQL

Use the lite route:

- keep the existing CodeMirror completion source initially;
- expose canonical Rust `analyze()` through Wasm;
- feed its diagnostics into `linter()`; and
- add semantic completions individually only when they demonstrate useful UX.

Use LSP only if OxQL tooling is intended for multiple editors or an
out-of-process service. For one embedded console editor, it adds protocol
machinery without reducing the hard language work.

## Recommendation

Both a Lezer port and an extracted Rust/Wasm parser are reasonable. The decision should turn on whether exact shared parsing or editor-native partial trees are more valuable.

Preferred route:

1. Port the grammar to Lezer so incomplete documents produce a usable tree and CodeMirror completion can use node/ancestor context.
2. Keep schema-aware completion in TS, but derive referenced timeseries and query/subquery scope from the tree instead of regexes.
3. Port the Rust parser corpus and add explicit Omicron grammar provenance/checking around version bumps.
4. Initially leave Shiki highlighting and server diagnostics in place; switch highlighting/live diagnostics only after the parser is proven.

If the immediate objective is strict client validation rather than richer editing, a Peggy port is the smaller first implementation and can later grow a cursor-hole rule. It is closer to the Rust grammar, but less naturally useful to CodeMirror while input is incomplete.

Do not compile `oximeter-db` directly to Wasm. If choosing Wasm, start with the lean parser-AST/conversion extraction and preserve a small browser boundary. Explicitly design partial-input/completion support rather than exposing only `parse(query)`. A useful spike would prove three things: the conversion boundary, the generated artifact/loading path, and a typed `completion_context(query, cursor)` API.

Before the parser work, the current completer has a few cheap parity fixes (`interpolate`, relevant special identifiers), but they do not address its structural limitations.

## References

- [Rust `peg` documentation](https://docs.rs/peg/latest/peg/) — macro grammar, embedded Rust actions, parse errors/expected tokens, prefix rules, and caching.
- [Peggy documentation](https://peggyjs.org/documentation.html) — build-time JS/TS parser generation, semantic actions, multiple start rules, and structured parse errors.
- [Lezer system guide](https://lezer.codemirror.net/docs/guide/) — editor-oriented incremental parsing and error recovery.
