# Oximeter benchmarks

## Field lookup

Filtering and pivoting OxQL field labels can take a significant fraction of overall query time, so we include a benchmark focusing on field lookup. This benchmark queries all timeseries for a given table, filtering on a far-future timestamp so that we don't exercise measurement lookup. Because field lookup latency varies with the number of field tables to be combined, we include metrics that use varying numbers of field types. In the interest of benchmarking realistic queries, this benchmark doesn't generate synthetic data, but instead provides scripts for the operator to back up real field data from a running rack and restore them into a test database.

To fetch field data:

```bash
$ mkdir -p /tmp/oximeter-field-bench
$ oximeter/db/benches/backup_field_tables.sh /tmp/oximeter-field-bench [port]
```

To restore into a test database. Note: take care not to restore into a real Oxide rack. For safety, the load script will fail if the destination database has nonzero rows.

```bash
$ oximeter/db/benches/load_field_tables.sh /tmp/oximeter-field-bench [port]
```

Then run the benchmark:

```bash
$ cargo bench --package oximeter-db --bench oxql -- --save-baseline main
```

To evaluate performance changes, run the benchmark using a new baseline:

```bash
$ cargo bench --package oximeter-db --bench oxql -- --save-baseline my-branch
```

Then compare with `critcmp`:

```bash
$ critcmp main my-branch
```
