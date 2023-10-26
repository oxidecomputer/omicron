# crdb-seed

This is a small utility that creates a seed tarball for our CockroachDB instance
in the temporary directory. It is used as a setup script for nextest (see
`.config/nextest.rs`).

This utility hashes inputs and attempts to reuse a tarball if it already exists
(see `digest_unique_to_schema` in `omicron/test-utils/src/dev/seed.rs`).

To invalidate the tarball and cause it to be recreated from scratch, set
`CRDB_SEED_INVALIDATE=1` in the environment.
