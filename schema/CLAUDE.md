# schema

CockroachDB schema and migrations for the control plane.

- `schema/crdb/dbinit.sql` — the full current schema, applied to a fresh database.
- `schema/crdb/<version>/` — versioned migrations applied to existing databases.

## Tips

- Read `schema/crdb/README.adoc` before making any schema change — it's the
  authoritative guide to the migration process.
- Use the `crdb-change` skill to generate schema changes and migrations; it
  keeps `dbinit.sql` and the migration in sync.
- Every schema change needs both a `dbinit.sql` edit and a migration; the two
  must produce the same result.
