# Overview

This migration changes the variants of the `instance_auto_restart` enum from
describing the set of _failure conditions_ under which an instance may be
automatically restarted (`Never`, `AllFailures`, and `SledFailuresOnly`) to a
more general description of the desired quality of service (currently `Never`
and `BestEffort`).

This change is mechanically complicated because Postgres and/or CRDB don't
support all the schema change primitives we might use to deprecate the old state
column. Specifically:

* CockroachDB doesn't support altering column types without enabling an
  experimental flag
  (see https://github.com/cockroachdb/cockroach/issues/49329?version=v22.1).
* Postgres doesn't support removing enum variants (adding and renaming are OK),
  so we can't shrink and directly reuse the existing auto-restart policy enum without
  leaving a set of "reserved"/"unused" variants around.
* Even if it did, Postgres doesn't support the `IF EXISTS` qualifier for many
  `ALTER TYPE` and `ALTER TABLE` statements, e.g. `ALTER TABLE RENAME COLUMN`
  and `ALTER TYPE RENAME TO`. There are ways to work around this (e.g. put the
  statement into a user-defined function or code block and catch the relevant
  exceptions from it), but CockroachDB v22.1.9 doesn't support UDFs (support
  was added in v22.2).

These limitations make it hard to change the schema idempotently. To get around
this, the change uses the following general procedure to change a column's type
from one enum to another:

1. Create a new temporary enum with the new variants, and a different name as
   the old type.
2. Create a new temporary column with the temporary enum type. (Adding a column
   supports `IF NOT EXISTS`).
3. Set the values of the temporary column based on the value of the old column.
4. Drop the old column.
5. Drop the old type.
6. Create a new enum with the new variants, and the same name as the original
   enum type (which we can now do, as the old type has been dropped).
7. Create a new column with the same name as the original column, and the new
   type --- again, we can do this now as the original column has been dropped.
8. Set the values of the new column based on the temporary column.
9. Drop the temporary column.
10. Drop the temporary type.

This is broadly similar to the approach used in the
[`separate-instance-and-vmm-states`](../separate-instance-and-vmm-states/)
migration, but using this technique allows the new enum to have the same name as
the old one.
