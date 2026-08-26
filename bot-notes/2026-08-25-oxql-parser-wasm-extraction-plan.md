# OxQL parser extraction and Wasm pipeline plan

Date: 2026-08-25

## Preliminary checkpoint

The parse-only feasibility checkpoint is implemented in the working copy. It
intentionally uses a temporary parser-AST bridge: `oximeter-db` still owns and
uses its existing parser, while the new target-independent `oxql` crate has a
parity copy. Do not remove the legacy parser or begin schema-analysis extraction
without an explicit go-ahead.

Results:

- A shared corpus has 11 accepted and 8 rejected queries. Native tests confirm
  that the legacy and extracted parsers agree on all 19.
- The extracted parser also tests structured syntax locations and the 4,096-byte
  limit.
- `oxql-wasm` builds as a release `--target web` package with wasm-bindgen
  0.2.114 and wasm-pack 0.15.0.
- Node 22 loads the generated ESM module from raw Wasm bytes and passes all 21
  JavaScript tests.
- The first release artifact is 171,813 bytes of Wasm and 8,936 bytes of
  JavaScript glue. The repository test script prints exact byte counts on each
  run.
- `cargo check` and clippy pass for the native and Wasm targets. The focused
  legacy/extracted parity test and workspace dependency policy check pass.

The next decision is whether to replace the server parser/AST with the shared
crate before extracting schema analysis. That migration is the main source-code
churn because syntax node declarations and execution methods are currently
interleaved.

## Completion-context go/no-go

The Rust completion-context experiment passes the 15 cases from the console
Lezer spike probe (checked in here as
`oximeter/oxql/test-data/completion-corpus.json`) through both the native API
and the generated Wasm package.

The API returns only language-derived data: completion-site kind, replacement
span, and timeseries names visible from the innermost query. Completion options,
schema descriptions, filtering, deduplication, and CodeMirror integration remain
client responsibilities.

The implementation repairs an incomplete prefix into a valid OxQL query and
runs the existing PEG. Query and timeseries AST nodes record source spans, which
allows the result to select the innermost query without a second parser. A
bounded recovery pass substitutes structurally valid, same-width operations for
completed earlier clauses that are still incomplete, preserving offsets. This
was required for the probe containing an incomplete inner filter and a complete
outer join.

Results:

- all 15 Lezer probe cases pass natively and through Wasm;
- all 19 parse parity cases continue to pass;
- all 37 Node 22 tests pass, including a UTF-16/UTF-8 cursor-offset boundary
  case;
- the release artifact is now 191,640 bytes of Wasm and 9,368 bytes of JS glue;
  and
- the go/no-go result establishes that a Wasm-only schema-analysis path is
  technically viable. It does not by itself establish that console should
  adopt it.

## Is console adoption worth it?

The 15 completion cases are a feasibility probe, not a sufficient product case.
They show that a strict PEG can be adapted to incomplete editor input and can
replace the Lezer spike without keeping two parsers. They do not show enough UX
improvement to justify a Wasm artifact, a cross-repository release process, and
an editor-facing recovery layer.

The current console regex completer already handles the common completion
paths: timeseries names after `get`, operations after a pipe, fields after
`filter`, alignment functions, group fields, reducers, and fields from grouped
subqueries. On inspection, the Rust context API improves six of the 15 probe
cases. It avoids identifier suggestions at literal positions and after complete
literals, is not confused by pipe and semicolon characters inside strings,
does not complete a finished outer `join`, and scopes fields to the innermost
subquery. Those are legitimate correctness improvements, but the regexes could
also be extended to cover several of them at much lower integration cost.

Completion-context parity therefore should not be the reason to adopt the Rust
package. Adoption is worthwhile only if the shared parser and analyzer enable a
meaningfully broader editor experience, such as:

- live, source-spanned syntax diagnostics before a query is submitted;
- live semantic diagnostics for missing timeseries and fields, incompatible
  literal types, and invalid alignment, grouping, and join operations;
- type-aware literal completion, including appropriate literal forms and
  special identifiers instead of field suggestions at every point in a filter;
- operation completion filtered by the derived table shape, so the editor
  offers operations that can legally follow the current pipeline;
- correctly scoped field completion through nested subqueries and intermediate
  operations, using the fields actually present at the cursor rather than every
  `get` matched anywhere in the document;
- reliable replacement spans and completion behavior with arbitrary cursor
  placement, incomplete surrounding clauses, quoted delimiters, and non-ASCII
  text; and
- automatic agreement with server syntax and shared semantic rules as OxQL
  evolves, without separately updating a client approximation.

Some desirable language-editor features do not fall out of this architecture.
The Wasm API does not provide CodeMirror syntax highlighting, folding,
structural selection, or incremental parsing. Those would need a separate
CodeMirror language implementation or additional Rust APIs, and should not be
counted as benefits of the current plan.

The costs to weigh against those improvements are:

- approximately 192 KiB of Wasm plus glue at the current checkpoint, along with
  loading, initialization, caching, and failure behavior in console;
- a versioned artifact or package distribution path between Omicron and
  console, plus CI for the Wasm toolchain;
- a compatibility policy between the console bundle, the server version, and
  schema catalogs that may be stale or permission-filtered;
- maintenance and fuzzing of incomplete-input recovery layered over a strict
  PEG—the current bounded repair strategy works for the probe but already
  contains a special recovery for an earlier incomplete clause; and
- the source churn needed to make the extracted parser and analyzer canonical
  in Omicron. Retaining parallel server and browser implementations is not an
  acceptable completed state.

### Suggested decision bar

Compare a console prototype against the current regex implementation, not just
against the Lezer probe. The prototype should demonstrate all of the following:

1. Live syntax and semantic diagnostics are fast and stable enough to display
   while editing, with useful spans and without excessive noise on incomplete
   queries.
2. Semantic completion produces visible improvements that are awkward for the
   regex implementation: type-aware filter values, legal next operations, and
   fields propagated through nested and transformed tables.
3. A larger adversarial corpus covers arbitrary cursor positions, malformed and
   partially typed queries, nested subqueries, escaped and unterminated strings,
   delimiters inside literals, and Unicode offsets. Fuzzing should verify that
   completion and analysis never panic or return invalid spans.
4. Browser integration measures artifact loading and per-keystroke latency and
   verifies behavior when Wasm or schemas are not yet available.
5. The artifact/versioning workflow is concrete enough that an OxQL grammar or
   analyzer change can update console without manual source synchronization.

If the intended console change remains only a more correct autocomplete
dispatcher, keep and improve the regex implementation. If the work ships shared
live diagnostics and semantic, table-shape-aware completion, the canonical Rust
parser/analyzer offers UX and maintenance benefits large enough to justify the
Wasm boundary.

## Goal

Extract the authoritative OxQL PEG parser from `oximeter-db` into a small reusable Rust crate, expose it to browser JavaScript through WebAssembly, and prove the complete build/load/call pipeline with JS tests that exercise representative accepted and rejected OxQL queries.

The extraction should leave data-dependent execution and database-specific planning/optimization in `oximeter-db`, share pure schema/operation analysis with browser clients, keep one authoritative grammar, and establish a foundation for a later cursor-aware completion API.

## Current structure and constraints

The current language implementation spans three kinds of logic that should be separated deliberately:

1. **Syntax and source-level rules** in `oximeter/db/src/oxql/ast/grammar.rs`, the AST node declarations under `ast/`, `Query::new`, and parse-error formatting. This includes the 4,096-byte query limit, literal validation, filter precedence, and the rule that every query/subquery begins with `get`.
2. **Schema- and operation-dependent analysis** in `oximeter/db/src/oxql/schema.rs` and the plan-node constructors under `oximeter/db/src/oxql/plan/`. These checks are deterministic from the query plus committed timeseries schemas and can run in a browser. Examples include missing schemas, invalid filter fields or literal types, grouping unaligned/nonnumeric/multidimensional input, invalid group fields, incompatible joins, and unsupported alignment operations.
3. **Data-dependent execution checks** in the table-operation implementations and database client. These require fetched samples and must remain server-side. Examples include grouping or joining empty results, the alignment upsampling ratio computed from actual timestamps, joins whose actual series keys do not match, and the one-million-row database fetch limit.

The planner already describes the desired static-analysis model. `TableSchema` carries fields, metric types, data types, and derived alignment state through each operation. `GroupBy::new`, `Join::new`, `Align::new`, and `Filter::new` reject invalid transformations before any measurements are fetched. This plan should share those rules rather than reimplement them independently for Wasm.

Important workspace constraints:

- `oximeter-db` cannot be built directly for `wasm32-unknown-unknown`; even its reduced feature set has many unconditional native/server dependencies.
- Wasm-capable crates must not depend on `omicron-workspace-hack`, which pulls a broad graph containing target-incompatible dependencies. Add the new core and wrapper crates to `.config/hakari.toml`'s traversal exclusions, following the existing no-std/tooling exceptions.
- The repository has no current Wasm toolchain or Node test job. Locally, `wasm-pack`, `wasm-bindgen`, and the `wasm32-unknown-unknown` target are not installed. Node is available, but CI should pin Node 22 to match console.
- Generated `.wasm` and JS glue are build artifacts. The spike should build into a temporary/`target` directory and must not check them in.

### Validation boundary

| Rule | Inputs required | Browser result |
| --- | --- | --- |
| Parse grammar, literals, query length, operation order | Query text | Error |
| Referenced timeseries does not exist in supplied catalog | Query + schema catalog | Error, with the caveat that a stale/permission-filtered client catalog may differ from the server |
| Filter identifier and literal type compatibility | Query + schema | Error |
| `timestamp`/`start_time`/`datum` applicability | Query + schema | Error |
| Alignment supported for the metric/data type | Query + schema | Error |
| `group_by` has one aligned, 1-D, noncumulative, numeric input and valid fields | Query + schema-derived table state | Error |
| `join` has at least two aligned 1-D inputs with matching periods and field schemas | Query + schema-derived table state | Error |
| Full-table-scan determination | Query + schema + optimizer + server-injected authorization filters | Warning/advisory initially; the server makes the final rejection |
| Filter-normalization/optimizer complexity limits | Query + optimizer | Candidate for the shared analyzer only when the server calls the same extracted implementation |
| Empty table/timeseries after filtering | Fetched data | Server only |
| Alignment upsampling limit | Actual sample timestamps | Server only |
| Join key/value/timestamp compatibility | Actual returned series | Server only |
| Database row limit | Query result cardinality | Server only |

Client analysis remains advisory. Submission must still use the API and display its authoritative error.

## Proposed architecture

Create two workspace crates:

### `oximeter/oxql` (`oxql`)

A target-independent language crate containing:

- the PEG grammar and parse-error formatting;
- source-oriented AST node types and display implementations;
- the query-length and structural invariants currently in `Query::new`;
- normalized input schema types that do not depend on `oximeter-db`, `oxql-types::Table`, or database clients;
- pure table-shape/alignment analysis for each operation; and
- structured diagnostics with stable codes, phase, severity, message, and source span where available.

Keep dependencies small and browser-compatible: `peg`, `chrono` only if timestamp parsing still needs it, `serde`, and a focused error crate if necessary. Avoid depending on `oximeter` merely for `TimeseriesName`; the grammar already recognizes its lexical form, and native code can convert the parsed newtype/string into `oximeter::TimeseriesName`. Review `uuid` features before using it because the workspace dependency enables UUID generation; parsing/validation may be simpler to keep inside the grammar or behind a reduced dependency.

Because the scope now includes schema analysis, prefer one canonical AST owned by this crate. Move source-level/pure manipulation methods with it. Leave table execution in `oximeter-db` as free functions or local extension traits over the shared AST types. A temporary parser-AST-to-database-AST conversion is acceptable while staging the extraction, but remove the parallel AST before considering the analysis extraction complete.

Define normalized schema inputs around the information analysis actually consumes, for example:

```rust
pub struct SchemaCatalog {
    pub timeseries: BTreeMap<TimeseriesName, InputSchema>,
}

pub struct InputSchema {
    pub fields: BTreeMap<String, FieldType>,
    pub metric_type: MetricType,
    pub data_type: DataType,
}
```

The exact enums may be moved from an existing lightweight crate if that crate remains Wasm-compatible. Otherwise define language-level enums here and add a native adapter from `oximeter::TimeseriesSchema`. Do not make the browser consume database result types.

The public Rust API should distinguish parsing from analysis:

```rust
pub fn parse(source: &str) -> Result<Query, Diagnostic>;
pub fn analyze(
    query: &Query,
    schemas: &SchemaCatalog,
) -> AnalysisResult;
```

`AnalysisResult` should include diagnostics, referenced timeseries, and the derived output table shapes. Expected query mistakes should be data, not panics or opaque `anyhow` chains.

### `oximeter/oxql-wasm` (`oxql-wasm`)

A thin `cdylib`/`rlib` wrapper containing only `wasm-bindgen` and boundary serialization. Export two initial functions:

```text
parse(query) -> { diagnostics, referencedTimeseries }
analyze(query, schemaCatalog) -> { diagnostics, referencedTimeseries, output }
```

Use `serde-wasm-bindgen` for nested schema/result objects instead of designing a large hand-written ABI. Keep the core crate unaware of `JsValue`. Return ordinary validation results for user errors; reserve thrown JS exceptions for binding/serialization/internal failures.

A later `completion_context(query, cursor)` API can reuse the same parser and schema model. It is outside the initial extraction unless a small prefix/cursor rule falls out naturally during the work.

## Implementation sequence

Keep each phase reviewable and green.

### 1. Establish behavioral fixtures before moving code

- Extract representative accepted/rejected query strings from `grammar.rs` tests into a data file such as `oximeter/oxql/test-data/query-corpus.json`.
- Add a native test that runs the corpus against the existing parser before extraction. Preserve focused Rust unit tests for literals, precedence, escaping, timestamps, and AST shape; the corpus complements rather than replaces them.
- Add cases for the 4,096-byte limit and formatted line/column diagnostics.
- Snapshot or assert the classifications of existing schema/planner failures listed in the validation table.

This gives the move a red/green parity harness and later lets Rust and JS execute the same cases.

### 2. Extract syntax into the core crate

- Add `oximeter/oxql` to workspace members/default members and root workspace dependencies.
- Move/copy the PEG, syntax AST declarations, pure display/manipulation code, and parser tests.
- Introduce a structured `Diagnostic`; preserve current user-facing messages initially to avoid changing API behavior during extraction.
- Remove `peg` from `oximeter-db`'s `oxql` feature after all call sites use the new crate.
- Re-export shared AST paths temporarily where that reduces churn.
- Convert database-only inherent implementations (`apply`, ClickHouse formatting, field/value execution) into `oximeter-db` extension traits or free functions. Do not move measurement/table execution into the core crate.
- Run the parser corpus through both implementations temporarily if useful, then delete the old grammar and duplicate AST declarations.

### 3. Prove the Wasm build and JS call boundary

- Add `oximeter/oxql-wasm` with target-gated bindings and no workspace-hack dependency.
- Add both new crates to Hakari traversal exclusions and regenerate/check workspace metadata.
- Pin `wasm-bindgen`/`serde-wasm-bindgen` in workspace dependencies and pin the `wasm-pack` CLI version used by CI.
- Implement `parse` first, with no schema input. Build a `--target web` ESM package and load that exact output from Node.
- Add the shared JS corpus test described below. Do not proceed to schema analysis until this path works end to end.

### 4. Extract the pure schema analyzer

- Introduce the normalized `SchemaCatalog`, `InputSchema`, field/data/metric types, table shape, and alignment state in `oxql`.
- Add conversion from `oximeter::TimeseriesSchema` in native code and from the console-facing JS schema shape at the Wasm boundary.
- Move the pure checks out of `plan/filter.rs`, `plan/align.rs`, `plan/group_by.rs`, `plan/join.rs`, and the relevant `Plan::plan_basic_table_op` dataflow into the core analyzer.
- Make the native planner consume the analyzer's derived state/results. There must be one implementation of each shared rule.
- Leave plan optimization, ClickHouse predicate generation, authorization-filter insertion, and execution in `oximeter-db` unless a later change demonstrates that a piece is both pure and useful to clients.
- Preserve server errors at the API boundary, mapping structured diagnostics into the existing error text where compatibility matters.

### 5. Expose analysis through Wasm

- Bind `analyze(query, schemas)` and return structured diagnostics plus derived output shapes.
- Add the schema-aware and runtime-boundary JS cases below.
- Record release Wasm size and initialization/call timing as observations, without setting a budget until measured. Queries are short, so correctness and loading ergonomics matter more than parse throughput.

### 6. Add CI and document consumer integration

- Add a focused Ubuntu GitHub Actions job rather than modifying the illumos/Linux Buildomat test battery. Install Node 22, the pinned `wasm32-unknown-unknown` target, and the pinned `wasm-pack` CLI.
- Run the same repository script used locally.
- Keep normal native tests in nextest so parser/analyzer behavior is covered even where Wasm tools are unavailable.
- Document the generated package surface and the remaining distribution decision (publish an npm package versus attach/fetch a versioned artifact). Distribution to console is a follow-up; the spike should not check in generated binaries.

## Wasm and JavaScript test spike

Add `tools/test_oxql_wasm.sh` (or an xtask only if the script becomes unwieldy). It should:

1. Validate that `wasm-pack`, Node, and the Rust target are available, with actionable install messages.
2. Create a temporary output directory with `mktemp -d` and clean it via a trap.
3. Run a release build of the browser-facing artifact:

   ```sh
   wasm-pack build oximeter/oxql-wasm \
     --target web \
     --release \
     --out-dir "$output_dir" \
     --out-name oxql
   ```

4. Pass the output path to `node --test oximeter/oxql-wasm/tests/js/*.test.mjs`.
5. Have the Node test dynamically import `oxql.js`, read `oxql_bg.wasm`, call the generated initializer with those bytes, and then call the exported Rust functions. This exercises the `web` ESM artifact intended for console rather than building a separate Node/CommonJS target.

Use Node's built-in test runner and assertions; a package manager or JS test dependency is unnecessary.

### Shared parse corpus

The JS test should read the same corpus as the native Rust test. At minimum include:

Accepted syntax:

- `get vm:cpu_busy`
- `get link:bytes_sent | filter timestamp > @now() - 5m`
- `get vm:cpu_busy | filter (project_id == "45c937fb-5e99-4a86-a95b-22bf30bf1507") && datum > 0.5`
- `get vm:cpu_busy | align mean_within(30s) | group_by [project_id], mean | last 10`
- `{ get vm:cpu_busy | align mean_within(30s); get vm:memory_used | align mean_within(30s) } | join`
- Queries covering hex integers, escaped strings, IPv4/IPv6 literals, XOR/negation precedence, and nested subqueries.

Rejected syntax:

- `get vm`
- `filter datum > 0`
- `get vm:cpu_busy | get vm:memory_used`
- `get vm:cpu_busy | align unknown(30s)`
- `get vm:cpu_busy | first 0`
- `get vm:cpu_busy | filter name ~= 10`
- Unbalanced parentheses/quotes and a query over 4,096 bytes.

Assert acceptance/rejection and, for a few failures, diagnostic phase plus line/column/message. Avoid making every test depend on complete prose when a diagnostic code is sufficient.

### Schema fixture and analysis cases

Create a small checked-in schema fixture with:

- `vm:cpu_busy` and `vm:memory_used`: gauge/double with identical UUID/string fields;
- `link:bytes_sent`: cumulative/integer with a string `link_name` field;
- `service:status`: gauge/string;
- a gauge metric with deliberately different fields for join failures.

Accepted analysis:

- A gauge metric filtered by a correctly typed field, aligned, grouped by an existing field, and limited.
- A cumulative counter that becomes delta input before alignment.
- A nested join whose inputs have identical fields and alignment periods.

Rejected analysis:

- Referencing an absent schema.
- Filtering on an unknown field or comparing a field to the wrong literal type.
- Filtering `timestamp` with a non-timestamp or using `datum` on incompatible/multidimensional output.
- Grouping before alignment, grouping a string metric, or grouping by a missing field.
- Joining unaligned inputs, different alignment periods, or different field schemas.
- `align interpolate(...)`: accepted by the grammar and rejected by current committed planning behavior because interpolation is unimplemented.

Explicit runtime-boundary cases should be accepted by client analysis:

- A filtered/aligned/grouped query whose filter could return no rows; only execution can reject grouping an empty table.
- A statically compatible join whose actual returned timeseries keys may not overlap.
- An alignment whose actual sample spacing may exceed the upsampling ratio.
- A query whose result cardinality may exceed the database row limit.

Naming these cases protects against gradually moving data-dependent guesses into client validation.

## Verification

Run after the relevant phases:

```sh
cargo fmt --all -- --check
cargo nextest run -p oxql
cargo nextest run -p oximeter-db
cargo check -p oxql-wasm --target wasm32-unknown-unknown
cargo clippy -p oxql -p oxql-wasm --all-targets -- -D warnings
cargo clippy -p oxql-wasm --target wasm32-unknown-unknown -- -D warnings
cargo xtask check-workspace-deps
tools/test_oxql_wasm.sh
```

Also run `cargo hakari generate` and review the workspace-hack diff; the new Wasm-capable crates should remain excluded. Before removing the old parser, run the shared corpus against old and new implementations and investigate any acceptance, AST-display, or diagnostic differences.

For the Wasm artifact, capture in the note/PR:

- `.wasm` and generated JS sizes from the release build;
- whether initialization succeeds in Node 22 using the `web` target;
- parse/analyze results for the shared corpus;
- any target-specific dependency or panic behavior; and
- the exact pinned Rust, `wasm-pack`, and `wasm-bindgen` versions.

## Risks and decisions

- **Canonical AST versus conversion bridge:** use a bridge only to stage the parser move. Shared schema analysis is easier to keep correct with a canonical core AST.
- **Workspace-hack:** accidentally including it will overwhelm or break the Wasm dependency graph. Treat the Hakari exclusion as part of the crate design, not a build workaround added later.
- **Schema freshness and permissions:** the browser catalog may lag or omit schemas the server can see. Use client diagnostics to help editing, while allowing the server to remain authoritative.
- **Server-injected filters:** full-scan policy depends on authorization filters added by the server. Report it as advisory until the client can supply identical context.
- **Time-dependent timestamp parsing:** the current grammar resolves `@now()` and time-only timestamps during parsing. Preserve behavior for server compatibility, but avoid serializing those evaluated values as a stable browser AST contract.
- **Error stability:** introduce diagnostic codes before changing prose. Console should key behavior on codes/phases and display messages, not parse strings.
- **Artifact distribution:** local/CI production is part of this plan; npm publication or pinned artifact delivery to console needs a separate decision after the spike proves size and ergonomics.
- **Incomplete input:** a strict PEG validates completed queries. Replacing autocomplete still needs a prefix/cursor-aware API; keep that follow-up visible without coupling it to the initial extraction.

## Completion criteria

- `oximeter-db` no longer owns a PEG grammar or duplicate syntax AST definitions.
- Native OxQL parsing and the Wasm export execute the same parser and pass the same accepted/rejected corpus.
- The shared core analyzer owns the selected schema/operation rules, and the native planner delegates to it rather than duplicating them.
- Runtime-only failures remain in server execution and are documented/tested as intentional client-analysis non-rejections.
- A repository command builds a release `--target web` package and tests it from Node 22 with real OxQL queries and schema fixtures.
- Focused CI runs the Wasm/JS pipeline without checking generated artifacts into source control.
- Native nextest, target check, clippy, workspace dependency checks, and the JS suite pass.
- The PR records artifact size and leaves a clear follow-up for console distribution and cursor-aware completion.

## References

- [`wasm-pack build` documentation](https://wasm-bindgen.github.io/wasm-pack/book/commands/build.html) for `--target web`, output directories, and profiles.
- [`wasm-bindgen` Serde guide](https://wasm-bindgen.github.io/wasm-bindgen/reference/arbitrary-data-with-serde.html) for passing nested Rust schema/result values through `JsValue` with `serde-wasm-bindgen`.
