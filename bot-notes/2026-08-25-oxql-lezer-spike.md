# OxQL Lezer spike

Date: 2026-08-25

> Agent-written note, originally in the console repo's unversioned notes dir.
> The spike code (`oxql-lezer-spike/`) lives beside the original note in the
> console repo and is not checked in here, but its 15 probe cases are: they are
> `oximeter/oxql/test-data/completion-corpus.json`, used by the Rust
> completion-context tests on this branch.

## Goal

Test whether a CodeMirror-native Lezer parser provides useful structure for incomplete OxQL queries while the authoritative Rust/Wasm parser is being explored separately.

The spike should answer:

1. Does error recovery preserve a useful cursor ancestry at each completion site?
2. Can the tree identify the timeseries visible to a filter or `group_by`, including nested queries, without scanning unrelated query branches?
3. How awkward is the Rust PEG grammar to express as an LR grammar?
4. Can Lezer remain an editor-only concrete syntax tree while Rust/Wasm owns completed-query validation and schema analysis?

## Experiment plan

- Build an isolated Lezer grammar covering representative OxQL query structure and literals.
- Parse complete and deliberately incomplete queries, recording error nodes and the node ancestry at the cursor.
- Prototype tree-based completion-context and in-scope-timeseries extraction.
- Compare the result with the current regex completer on nested queries, strings containing delimiters, logical `||`, and malformed input.
- Keep the experiment out of `OxqlEditor` until the tree behavior is understood.

## Findings

### What was built

The isolated experiment is in `oxql-lezer-spike/` beside this note. It contains:

- a 71-line Lezer grammar covering pipelines, grouped queries, all table operations, filter precedence, and the editor-relevant shape of literals;
- generated parse tables;
- a direct parser probe with 15 asserted incomplete-query, completion-site, and scope cases;
- a CodeMirror `LRLanguage` probe proving the generated parser is available through `syntaxTree(state)`; and
- a seven-query complete-input corpus covering nested queries, precedence, timestamps, hex values, and quoted delimiters.

The generated parser is 6,704 bytes before minification and 3,065 bytes gzipped. The grammar source is 1,926 bytes. Artifact size is unlikely to affect the decision.

### Recovery and cursor context

Lezer retained useful ancestors for the completion sites tested:

| Incomplete input | Cursor ancestry that remained available |
| --- | --- |
| `get ` | `Get > Operation > Pipeline > Query` |
| `get hardware_component:` | `TimeseriesName > TimeseriesList > Get` |
| `... \| ` | `Pipeline > Query` with an inserted error node |
| `... \| filter ` | `Filter > Operation > Pipeline > Query` |
| `... filter chassis_kind == ` | `Comparison > ... > Filter` |
| `... \| align mean_` | error node inside `Align` |
| `... \| group_by [sl` | `Identifier > FieldList > GroupBy` |
| `... \| group_by [sled_id], ` | `GroupBy > Operation > Pipeline` |

The tree correctly kept `|` and `;` inside quoted strings out of the pipeline structure. The current regex completer treats those characters as clause boundaries.

Nearest-`Query` traversal also gave the desired nested scope in the tested cases:

- after `{ get a:b; get c:d } | filter `, both `a:b` and `c:d` were visible;
- inside the second branch of `{ get a:b; get c:d | filter ...`, only `c:d` was visible; and
- an incomplete filter in one branch did not prevent the outer query and later `join` from retaining structure.

There was one small cursor-handling wrinkle: skipped trailing whitespace is absent from a Lezer tree. When no recovery node exists at the cursor, the helper must walk backward to the nearest concrete node. This took a few lines and behaved consistently in the probes.

### The grammar does not implement autocomplete

Installing the grammar as an `LRLanguage` only makes a syntax tree available to CodeMirror. It does not provide completion options or decide when to offer them.

The experiment still needed an explicit editor policy layer to:

1. resolve the tree node at the cursor;
2. classify it as a timeseries name, table operation, filter identifier, filter literal, alignment method, grouping field, or reducer site;
3. find the relevant query scope and referenced timeseries;
4. determine the replacement range/prefix;
5. map a site to static keyword/snippet options; and
6. look up dynamic timeseries and field options in the schema catalog.

The prototype classifier and scope traversal are small, but they are application code rather than generated grammar behavior. A production `CompletionSource` would call `syntaxTree(context.state)`, perform this classification, and then return CodeMirror completion records. Partial keywords such as `mean_` may be represented as error nodes, so CodeMirror's `matchBefore` (or an equivalent source-text lookup) remains necessary for the replacement range and prefix.

The schema problem is deeper than collecting every preceding `get`. A filter immediately after one `get` can use that timeseries schema, and a filter after a grouped query can use the group inputs. After alignment, grouping, or joining, however, the available columns depend on the derived table shape. Accurate completions there need either:

- a client-side operation/schema propagation layer;
- output-shape information from the proposed Wasm analyzer; or
- an authoritative Wasm `completion_context`/prefix-analysis API.

Lezer answers where the cursor is and which incomplete query contains it. It cannot answer which identifiers are valid without one of those semantic layers.

### Relationship to Rust/Wasm

The experiment makes the two parsers look complementary:

- Lezer owns a deliberately permissive concrete syntax tree for incomplete editor input.
- Rust/Wasm owns completed-query acceptance, literal validation, structural invariants, and schema/operation analysis.
- The completion source combines Lezer cursor context with schema or analyzer output to produce options.

The Lezer grammar intentionally does not reproduce integer ranges, UUID/IP validation, timestamp validation, string unescaping, nonzero limits, or the rule that each query starts with `get`. Keeping those in the authoritative parser makes the editor grammar smaller and avoids treating an error-free Lezer tree as proof that a query is valid.

This does leave a shallow duplicate of the language's concrete structure. Grammar churn is low, and the useful editor grammar is much smaller than the Rust PEG because it has no AST actions or semantic validation. A shared accepted-query corpus plus a grammar review on `OMICRON_VERSION` bumps should be sufficient synchronization if this approach is adopted.

### Conclusion

The Lezer spike was informative. It demonstrated good recovery and nested-query scoping at the exact places the regex completer is fragile. It also confirmed that adopting Lezer does not remove the need to design autocomplete: it replaces delimiter/regex inference with a structured input to a still-custom completion source.

The next discriminating experiment depends on what the Wasm spike returns. If it exposes only strict `parse`/`analyze`, combine it with Lezer and prototype a tree-based `CompletionSource`. If it exposes useful prefix or cursor analysis, run the same 15 incomplete cases against that API and compare scope/context quality before committing to a second grammar.

## References

- [Lezer system guide](https://lezer.codemirror.net/docs/guide/)
- [Lezer reference manual](https://lezer.codemirror.net/docs/ref/)
