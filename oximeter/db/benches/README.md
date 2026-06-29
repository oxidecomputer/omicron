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

Then run the benchmark. `BENCH_METRIC` selects between server-side wall-clock (`latency`) and CPU time (`cpu_time`):

```bash
$ BENCH_METRIC=latency cargo bench --package oximeter-db --bench oxql_field -- --save-baseline main
```

To evaluate performance changes, run the benchmark using a new baseline:

```bash
$ BENCH_METRIC=latency cargo bench --package oximeter-db --bench oxql_field -- --save-baseline my-branch
```

Then compare with `critcmp`:

```bash
$ critcmp main my-branch
```

## Measurement query

We have a separate benchmark that measures the performance of combined field and measurement lookup, fetching a set of representative series using `| last 1` to simulate the use case of fetching recent metrics to ship to Prometheus or similar. This benchmark requires backing up and restoring measurement tables. Use a limited time window, since these tables grow to tens of gigabytes or more on real racks.

To fetch measurement data:

```bash
$ mkdir -p /tmp/oximeter-measurement-bench
$ START=2026-05-01T00:00:00
$ END=2026-05-01T01:00:00
$ oximeter/db/benches/backup_measurement_tables.sh /tmp/oximeter-measurement-bench measurements_cumulativeu64 $START $END [port]
$ oximeter/db/benches/backup_measurement_tables.sh /tmp/oximeter-measurement-bench measurements_f32 $START $END [port]
```

To restore into a test database:

```bash
$ oximeter/db/benches/load_measurement_tables.sh /tmp/oximeter-measurement-bench measurements_cumulativeu64 [port]
$ oximeter/db/benches/load_measurement_tables.sh /tmp/oximeter-measurement-bench measurements_f32 [port]
```

Run the benchmark. `OXQL_BENCH_START_TIME` and `OXQL_BENCH_END_TIME` should fall within the window you backed, in `YYYY-MM-DDTHH:MM:SS` format:

```bash
$ OXQL_BENCH_START_TIME=$START OXQL_BENCH_END_TIME=$END BENCH_METRIC=latency \
    cargo bench --package oximeter-db --bench oxql_measurement -- --save-baseline main
```
