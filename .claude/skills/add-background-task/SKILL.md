---
name: add-background-task
description: Add a new Nexus background task. Use when the user wants to create a periodic background task in Nexus that runs on a timer.
---

# Add a Nexus background task

All background tasks live in Nexus. A task implements the `BackgroundTask` trait (`nexus/src/app/background/mod.rs`), runs on a configurable period, and reports status as `serde_json::Value`.

## General approach

There are many existing background tasks in `nexus/src/app/background/tasks/`. Before writing anything, read a few tasks that are similar in shape to the one you're adding (e.g., a simple periodic cleanup vs. a task that watches a channel). Use those as models for structure, naming, logging, error handling, and status reporting. The goal is to conform to the patterns already in use, not to invent new ones.

## Checklist

These are the touch points for adding a new background task. Follow them in order.

### 1. Status type (`nexus/types/src/internal_api/background.rs`)

Define a struct for the task's activation status. Derive `Clone, Debug, Deserialize, Serialize, PartialEq, Eq`. For errors, use `Option<String>` if the task can only fail in one way per activation, or `Vec<String>` if it accumulates multiple independent errors. Match what similar tasks do.

### 2. Task implementation (`nexus/src/app/background/tasks/<name>.rs`)

Create the task module. The struct holds whatever state it needs (typically `Arc<DataStore>` plus config). Implement `BackgroundTask::activate` by delegating to an `async fn actually_activate(&mut self, opctx) -> YourStatus` method, then serialize the status to `serde_json::Value`. The `actually_activate` pattern makes unit testing easy without going through the trait.

Logging conventions: `debug` when there's nothing to do, `info` when routine work was done, `warn` when the work done indicates something is wrong (e.g., cleaning up after a crash), `error` on failure. Log errors as structured fields with the `; &err` slog syntax (which uses the `SlogInlineError` trait), not by interpolating into the message string. For the error string in the status struct, use `InlineErrorChain::new(&err).to_string()` (from `slog_error_chain`) to capture the full cause chain. Status error strings should not repeat the task name — omdb already shows which task you're looking at.

If the task takes config values that need conversion or validation (e.g., converting a `Duration` to `TimeDelta`, or checking a numeric range), do it once in `new()` and store the validated form. Don't re-validate on every activation — if the config is invalid, panic in `new()` with a message that includes the invalid value.

Include a unit test in the same file using `TestDatabase::new_with_datastore` that calls `actually_activate` directly. If the task has a datastore method, a single test exercising the task end-to-end is usually sufficient — don't add a redundant test for the datastore method separately unless it has complex logic worth testing in isolation.

For tasks that mutate or delete a bounded set of rows, the test should prove which items were affected, not just how many. Seed a mix of rows that should be affected, rows that should be skipped because they're on the safe side of the cutoff or predicate, and rows that should be skipped because they're structurally ineligible. Assert exact identities of the affected and surviving rows after each activation. If the task uses ordering plus a per-activation limit, make the first activation hit the limit and verify that the highest-priority eligible items were chosen, then run a second activation to verify the remainder, and a final activation to verify the no-op case. When eligibility depends on time, set timestamps explicitly in the test rather than relying on sleeps. When selection depends on multiple sort keys, include a case that exercises the tiebreaker. Prefer assertions on row IDs or full records over aggregate counts and status fields.

### 3. Register the module (`nexus/src/app/background/tasks/mod.rs`)

Add `pub mod <name>;` in alphabetical order.

### 4. Activator (`nexus/background-task-interface/src/init.rs`)

Add `pub task_<name>: Activator` to the `BackgroundTasks` struct, maintaining alphabetical order among the task fields.

### 5. Config (`nexus-config/src/nexus_config.rs`)

Add a config struct (e.g., `YourTaskConfig`) with at minimum `period_secs: Duration` (using `#[serde_as(as = "DurationSeconds<u64>")]`). If the task does bounded work per activation, name the limit field `max_<past_tense_verb>_per_activation` (e.g., `max_deleted_per_activation`, `max_timed_out_per_activation`) to match existing conventions. Add the field to `BackgroundTaskConfig`. Update the test config literal and expected parse output at the bottom of the file.

### 6. Config files

Add the new config fields to all of these:
- `nexus/examples/config.toml`
- `nexus/examples/config-second.toml`
- `nexus/tests/config.test.toml`
- `smf/nexus/single-sled/config-partial.toml`
- `smf/nexus/multi-sled/config-partial.toml`

### 7. Wire up in `nexus/src/app/background/init.rs`

- Import the task module.
- Add `Activator::new()` in the `BackgroundTasks` constructor.
- Destructure it in the `start` method.
- Call `driver.register(TaskDefinition { ... })` with the task. The last task registered should pass `datastore` by move (not `.clone()`), so adjust the previous last task if needed.
- If extra data is needed from `BackgroundTasksData`, add the field there and plumb it from `nexus/src/app/mod.rs`.

### 8. Schema migration (if needed)

If the task needs a new index or schema change to support its query, add a migration under `schema/crdb/`. See `schema/crdb/README.adoc` for the procedure. Also update `dbinit.sql` and bump the version in `nexus/db-model/src/schema_versions.rs`.

### 9. Datastore method (if needed)

If the task needs a new query, add it in the appropriate `nexus/db-queries/src/db/datastore/` file. Prefer the Diesel typed DSL over raw SQL (`diesel::sql_query`) for queries and test helpers. Only fall back to raw SQL when the DSL genuinely can't express the query.

If the task modifies rows that other code paths also modify, think about races: what happens if both run concurrently on the same row? Both paths should typically guard their writes so only one succeeds.

### 10. omdb output (`dev-tools/omdb/src/bin/omdb/nexus.rs`)

Add a `print_task_<name>` function and wire it into the match in `print_task_details` (alphabetical order). Import the status type. Use the `const_max_len` + `WIDTH` pattern to align columns:

```rust
const LABEL: &str = "label:";
const WIDTH: usize = const_max_len(&[LABEL, ...]) + 1;
println!("    {LABEL:<WIDTH$}{}", status.field);
```

### 11. Update test output (`dev-tools/omdb/tests/`)

Run the omdb tests with `EXPECTORATE=overwrite` to update the expected output snapshots (`env.out` and `successes.out`):

```
EXPECTORATE=overwrite cargo nextest run -p omicron-omdb
```

Review the diff to make sure only your new task's output was added.

### 12. Verify

- `cargo check -p omicron-nexus --all-targets`
- `cargo fmt`
- `cargo xtask clippy`
- Run the new task's unit tests
- Run the omdb tests: `cargo nextest run -p omicron-omdb`
