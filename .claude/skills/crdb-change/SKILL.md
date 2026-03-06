---
name: crdb-change
description: Generate CockroachDB SQL and migrations for schema changes. Use when creating migrations, updating the database schema, or when the user mentions migrations, schema changes, or dbinit.sql.
---

# CockroachDB database changes

Generate database changes for this repository. This includes changes to:

- `schema/crdb/dbinit.sql`
- Migrations for changes

## Instructions

Follow these steps in order. Do not skip ahead.

### Step 1: Ascertain the scope of the request

If not already provided, prompt the user whether they'd like to:

- **Perform both a dbinit.sql change and a migration**: If this is the case, then:
  1. Ask the user to describe the changes they'd like to perform to `dbinit.sql`.
  2. Make those changes, asking the user followup questions as necessary.
  3. Go directly to step 3, with the scope of the migration being to cover those changes.
- **Write migrations for pre-existing changes**: In this case, changes need to be fetched from version control. Go to step 2.

### Step 2: Get the diff

Check if `.jj` exists in the repository to determine whether to use jj or git commands.

Prompt the user to ask where the schema changes are:

- **Uncommitted changes**: Changes not yet committed.
  - git: `git diff -- schema/crdb/dbinit.sql` (unstaged) or `git diff --cached -- schema/crdb/dbinit.sql` (staged)
  - jj: `jj diff -- schema/crdb/dbinit.sql`

- **This commit only** (stacked diff workflow): Changes are in the current commit only.
  - git: `git diff HEAD^ -- schema/crdb/dbinit.sql`
  - jj: `jj diff --from @-- -- schema/crdb/dbinit.sql`

- **This branch** (feature branch workflow): Changes span the entire branch.
  - git: `git diff $(git merge-base HEAD main) -- schema/crdb/dbinit.sql`
  - jj: `jj diff --from 'fork_point(trunk() | @)' -- schema/crdb/dbinit.sql`

If the diff doesn't show anything, ask the user which ref to diff from.

### Step 3: Create migration folder

Create a new folder under `schema/crdb/` using the provided name or a short descriptive name derived from the schema changes.

Use existing folder names in `schema/crdb/` as examples for naming conventions.

NOTE: The numbered folders, e.g. 1.0.0, are for legacy support only. No additional numbered directories should be added.

### Step 4: Write migration files

Based on the diff from step 1, write migration files in order:

- Use `up01.sql`, `up02.sql` etc. (zero-padded) if you have more than 10 files.
- Use `up1.sql`, `up2.sql` etc. if you have 10 or fewer files.
- For Data Definition Language (DDL) statements, **one statement per file!**
- For Data Modifying Language (DML) statements, multiple statements are allowed per file.
- For `ALTER TABLE` with multiple columns, you can add them all in one statement.
- When adding `NOT NULL` columns to existing tables, add temporary defaults, then remove them in later migration files.
- Use `IF NOT EXISTS` for idempotency where supported.
- Individual `up.sql` files are executed within a transaction (this always happens), and should be idempotent (this is an expectation that the migration author must uphold, with, e.g. `IF NOT EXISTS`).

### Step 5: Update dbinit.sql version

Bump the version number at the end of `schema/crdb/dbinit.sql`.

### Step 6: Update SCHEMA_VERSION

In `nexus/db-model/src/schema_versions.rs`, bump `SCHEMA_VERSION`.

### Step 7: Add to KNOWN_VERSIONS

In `nexus/db-model/src/schema_versions.rs`, add the new version to the `KNOWN_VERSIONS` list.

### Step 8: Generate verification files

Run `EXPECTORATE=overwrite cargo nextest run -p nexus-db-model 'test_migration_verification_files'` to auto-generate `.verify.sql` files for any migration steps that contain backfill-prone DDL (`CREATE INDEX`, `ADD CONSTRAINT`, `ALTER COLUMN SET NOT NULL`, or `ADD COLUMN` with `NOT NULL` / non-null `DEFAULT` / `STORED` computed columns). Check in any generated files alongside the migration. If your migration doesn't contain backfill-prone DDL, no files are generated and this step is a no-op.

### Step 9: Test the migration

Run `cargo nextest run -p omicron-nexus schema` to verify that the migration is correct.

## Common issues

- Don't guess table names—use the diff from step 1 to find the correct table.
- When adding constraints, always use `IF NOT EXISTS` for idempotency.
- For columns with defaults in migration but not in final schema, add the defaults during migration then remove them.
- Don't skip step 1—always run the diff command first to understand what needs to be migrated.

## Data migration tests

When creating a migration that affects existing data (like adding columns to existing tables), also add a data migration test in `nexus/tests/integration_tests/schema.rs`:

- Add `before_X_0_0` function to create test data in the old format.
- Add `after_X_0_0` function to verify the migration worked correctly.
- Add the version to the `get_migration_checks()` map.

This ensures old rows can be migrated smoothly in production.

## Reference

Consult `schema/crdb/README.adoc` for more information.
