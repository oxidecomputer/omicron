# Overview

This migration changes the variants of the `instance_auto_restart` enum from
describing the set of _failure conditions_ under which an instance may be
automatically restarted (`Never`, `AllFailures`, and `SledFailuresOnly`) to a
more general description of the desired quality of service (currently `Never`
and `BestEffort`).

This change is mechanically because Postgres and/or CRDB don't support all the schema change primitives we
might use to deprecate the old state column. Specifically:

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

1. Create a new enum with the new variants.
2. Create a new temporary column to hold the old object state. (Adding a column
  supports `IF NOT EXISTS`).
3. Copy the old object state to the temporary column.
4. Drop the old column (this supports `IF EXISTS`).
5. Recreate the state column with the new type.
6. Populate the column's values using the data saved in the temporary column.
7. Drop the temporary column.
8. Drop the old enum type.

This is broadly similar to the approach used in the
[`separate-instance-and-vmm-states`](../separate-instance-and-vmm-states/)
migration. 
