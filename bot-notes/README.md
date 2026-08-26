# bot-notes

Agent-written working notes for the OxQL parser extraction / Wasm spike on this
branch. Normally these live in an unversioned `.claude/notes/` dir (some in
this repo, some in the console repo); they're checked in here so the spike is
self-describing for humans and for agents picking the work back up.

Reading order:

1. [`2026-08-25-oxql-client-parser-options.md`](2026-08-25-oxql-client-parser-options.md)
   — the initial survey: current console regex completer, the authoritative
   Rust PEG's structure and churn history, TypeScript-port vs. Lezer vs. Wasm
   options, and the CodeMirror integration model.
2. [`2026-08-25-oxql-lezer-spike.md`](2026-08-25-oxql-lezer-spike.md) — an
   isolated Lezer grammar spike (code in the console repo) testing error
   recovery and cursor context on incomplete queries. Its 15 probe cases are
   checked in here as `oximeter/oxql/test-data/completion-corpus.json`.
3. [`2026-08-25-oxql-parser-wasm-extraction-plan.md`](2026-08-25-oxql-parser-wasm-extraction-plan.md)
   — the full extraction plan for this repo plus the results of the two
   checkpoints implemented on this branch: the parse-only Wasm pipeline and the
   cursor-aware completion-context API.
4. [`2026-08-25-oxql-wasm-decision.md`](2026-08-25-oxql-wasm-decision.md) — the
   outcome: **not now**. The pipeline is feasible, but the completion win over
   an improved regex completer is small, grammar churn is near zero, and the
   costs (a ~192 KB Wasm artifact, cross-repo versioning, a permanent
   prefix-repair layer over the strict PEG) are structural. Includes the
   decision bar to apply if this is revisited and the implementation details
   worth remembering (prefix-repair strategy, width-preserving clause recovery,
   UTF-16/UTF-8 span conversion, Hakari exclusion).

State of the branch: the spike intentionally leaves `oximeter-db`'s parser in
place and gives the new `oximeter/oxql` crate a parity copy, verified against a
shared corpus. `tools/test_oxql_wasm.sh` builds the release Wasm package and
runs the Node test suite against it.
