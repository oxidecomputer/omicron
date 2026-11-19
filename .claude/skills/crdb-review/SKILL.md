---
name: crdb-review
description: Review database code for known schema and pagination mistakes. Use when the user asks to review database queries, schema changes, or migrations, or when reviewing a diff that touches nexus/db-queries, nexus/db-model, nexus/db-schema, or schema/crdb.
---

# Database review

Review database code for known schema and pagination mistakes in Omicron.

## Instructions

### Step 1: Read the guides

Read both of these in full:

- `schema/crdb/README.adoc`: how to change the database schema, including constraints on schema updates.
- `docs/db-pagination.adoc`: how pagination works, the invariants it depends on, and common mistakes.

Do not work from memory! The guides are the source of truth and may have gained rules or mistakes since your last memory or training data.

### Step 2: Determine the code under review

If the user pointed at specific files, queries, migrations, or a PR, review those. Otherwise, prompt the user to ascertain the scope of the diff. This can be:

- **Uncommitted changes**: Changes not yet committed.
  - git: `git diff` (unstaged) or `git diff --cached` (staged)
  - jj: `jj diff --git`

- **This commit only** (stacked diff workflow): Changes are in the current commit only.
  - git: `git diff HEAD^`
  - jj: `jj diff --git --from @--`

- **This branch** (feature branch workflow): Changes span the entire branch.
  - git: `git diff $(git merge-base HEAD main)`
  - jj: `jj diff --git --from 'fork_point(trunk() | @)'`

Focus on datastore code (`nexus/db-queries`), models and schema definitions (`nexus/db-model`, `nexus/db-schema`), `schema/crdb/dbinit.sql`, and migrations under `schema/crdb/`. Also consider callers that page through datastore functions (for example, `Paginator` loops in Nexus background tasks or omdb), since pagination mistakes can occur in the caller rather than the query.

### Step 3: Check against each rule and mistake

For each schema-update constraint in `schema/crdb/README.adoc`, check whether the schema changes and migrations under review violate it.

For each mistake in `docs/db-pagination.adoc`, check whether the code under review exhibits it. In particular, for every paginated query, check:

- The paging key against the table's primary key and the query's equality filters, to confirm it is unique and immutable.
- That nothing after `paginated()` or `paginated_multicolumn()` overrides the `ORDER BY`.
- That the marker returned to `found_batch` comes from exactly the paging columns, in order.
- That the batch passed to `found_batch` is exactly the rows the query returned, with no filtering, decoding, re-sorting, or collecting into a map first.

Also flag bulk reads that load every matching row in one query.

### Step 4: Report findings

For each finding, report:

* The rule or mistake name from the guide.
* The location (`file:line`).
* Why the code is affected, in terms of the specific rows that would be skipped, duplicated, or truncated, or the specific upgrade step that would fail.
* A suggested fix, preferring the helpers and approaches the guides recommend (for example, `paginated_multicolumn` or `paginated_multicolumn3` for composite keys, or a separate migration step).

If a finding is latent (for example, it only triggers once a table exceeds one page, or relies on an application-level invariant), say so explicitly, and describe the data that would trigger it.
