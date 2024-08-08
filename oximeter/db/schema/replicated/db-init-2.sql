/* We purposefully split our DB schema into two *disjoint* files:
 * `db-init-1.sql` and `db-init-2.sql`. The purpose of this split is to shorten
 * the duration of our replicated tests. These tests only use a subset of the
 * tables defined in the full schema. We put the tables used by the replicated
 * tests in `db-init-1.sql`, and the remainder of the tables in `db-init-2.sql`.
 * This minimizes test time by reducing the cost to load a schema. In
 * production, we load `db-init-1.sql` followed by `db-init-2.sql` so we have
 * the full schema. If we end up needing to use more tables in replicated tests
 * we can go  ahead and move them into `db-init-1.sql`, removing them from
 * `db-init-2.sql`. Conversely, if we stop using given tables in our tests we
 * can move them from `db-init-1.sql` into `db-init-2.sql` and keep our test
 * times minimal.

 * The reason to keep the two files disjoint is so that we don't have to
 * maintain consistency between table definitions. All tables are defined
 * once. However, in order to write a test that ensures the tables are in fact
 * disjoint, we must create the `oximeter` database in both files so we can load
 * them in isolation.
 */

CREATE DATABASE IF NOT EXISTS oximeter ON CLUSTER oximeter_cluster;

/* The measurement tables contain all individual samples from each timeseries.
 *
 * Each table stores a single datum type, and otherwise contains nearly the same
 * structure. The primary sorting key is on the timeseries name, key, and then
 * timestamp, so that all timeseries from the same schema are grouped, followed
 * by all samples from the same timeseries.
 *
 * This reflects that one usually looks up the _key_ in one or more field table,
 * and then uses that to index quickly into the measurements tables.
 */
CREATE TABLE IF NOT EXISTS oximeter.measurements_bool_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum Nullable(UInt8)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_bool_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS oximeter.measurements_bool ON CLUSTER oximeter_cluster
AS oximeter.measurements_bool_local
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_bool_local', xxHash64(splitByChar(':', timeseries_name)[1]));

CREATE TABLE IF NOT EXISTS oximeter.measurements_i8_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum Nullable(Int8)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_i8_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS oximeter.measurements_i8 ON CLUSTER oximeter_cluster
AS oximeter.measurements_i8_local
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_i8_local', xxHash64(splitByChar(':', timeseries_name)[1]));

CREATE TABLE IF NOT EXISTS oximeter.measurements_u8_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum Nullable(UInt8)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_u8_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS oximeter.measurements_u8 ON CLUSTER oximeter_cluster
AS oximeter.measurements_u8_local
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_u8_local', xxHash64(splitByChar(':', timeseries_name)[1]));

CREATE TABLE IF NOT EXISTS oximeter.measurements_i16_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum Nullable(Int16)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_i16_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS oximeter.measurements_i16 ON CLUSTER oximeter_cluster
AS oximeter.measurements_i16_local
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_i16_local', xxHash64(splitByChar(':', timeseries_name)[1]));

CREATE TABLE IF NOT EXISTS oximeter.measurements_u16_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum Nullable(UInt16)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_u16_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS oximeter.measurements_u16 ON CLUSTER oximeter_cluster
AS oximeter.measurements_u16_local
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_u16_local', xxHash64(splitByChar(':', timeseries_name)[1]));

CREATE TABLE IF NOT EXISTS oximeter.measurements_i32_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum Nullable(Int32)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_i32_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS oximeter.measurements_i32 ON CLUSTER oximeter_cluster
AS oximeter.measurements_i32_local
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_i32_local', xxHash64(splitByChar(':', timeseries_name)[1]));

CREATE TABLE IF NOT EXISTS oximeter.measurements_u32_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum Nullable(UInt32)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_u32_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS oximeter.measurements_u32 ON CLUSTER oximeter_cluster
AS oximeter.measurements_u32_local
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_u32_local', xxHash64(splitByChar(':', timeseries_name)[1]));

CREATE TABLE IF NOT EXISTS oximeter.measurements_i64_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum Nullable(Int64)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_i64_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS oximeter.measurements_i64 ON CLUSTER oximeter_cluster
AS oximeter.measurements_i64_local
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_i64_local', xxHash64(splitByChar(':', timeseries_name)[1]));

CREATE TABLE IF NOT EXISTS oximeter.measurements_u64_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum Nullable(UInt64)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_u64_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS oximeter.measurements_u64 ON CLUSTER oximeter_cluster
AS oximeter.measurements_u64_local
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_u64_local', xxHash64(splitByChar(':', timeseries_name)[1]));

CREATE TABLE IF NOT EXISTS oximeter.measurements_f32_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum Nullable(Float32)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_f32_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS oximeter.measurements_f32 ON CLUSTER oximeter_cluster
AS oximeter.measurements_f32_local
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_f32_local', xxHash64(splitByChar(':', timeseries_name)[1]));

CREATE TABLE IF NOT EXISTS oximeter.measurements_f64_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum Nullable(Float64)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_f64_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS oximeter.measurements_f64 ON CLUSTER oximeter_cluster
AS oximeter.measurements_f64_local
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_f64_local', xxHash64(splitByChar(':', timeseries_name)[1]));

CREATE TABLE IF NOT EXISTS oximeter.measurements_string_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum Nullable(String)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_string_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS oximeter.measurements_string ON CLUSTER oximeter_cluster
AS oximeter.measurements_string_local
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_string_local', xxHash64(splitByChar(':', timeseries_name)[1]));

CREATE TABLE IF NOT EXISTS oximeter.measurements_bytes_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum Array(UInt8)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_bytes_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS oximeter.measurements_bytes ON CLUSTER oximeter_cluster
AS oximeter.measurements_bytes_local
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_bytes_local', xxHash64(splitByChar(':', timeseries_name)[1]));

CREATE TABLE IF NOT EXISTS oximeter.measurements_cumulativei64_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    datum Nullable(Int64)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_cumulativei64_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, start_time, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS oximeter.measurements_cumulativei64 ON CLUSTER oximeter_cluster
AS oximeter.measurements_cumulativei64_local
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_cumulativei64_local', xxHash64(splitByChar(':', timeseries_name)[1]));

CREATE TABLE IF NOT EXISTS oximeter.measurements_cumulativeu64_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    datum Nullable(UInt64)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_cumulativeu64_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, start_time, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS oximeter.measurements_cumulativeu64 ON CLUSTER oximeter_cluster
AS oximeter.measurements_cumulativeu64_local
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_cumulativeu64_local', xxHash64(splitByChar(':', timeseries_name)[1]));

CREATE TABLE IF NOT EXISTS oximeter.measurements_cumulativef32_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    datum Nullable(Float32)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_cumulativef32_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, start_time, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS oximeter.measurements_cumulativef32 ON CLUSTER oximeter_cluster
AS oximeter.measurements_cumulativef32_local
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_cumulativef32_local', xxHash64(splitByChar(':', timeseries_name)[1]));

CREATE TABLE IF NOT EXISTS oximeter.measurements_histogrami8_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    bins Array(Int8),
    counts Array(UInt64),
    min Int8,
    max Int8,
    sum_of_samples Int64,
    squared_mean Float64,
    p50_marker_heights Array(Float64),
    p50_marker_positions Array(UInt64),
    p50_desired_marker_positions Array(Float64),
    p90_marker_heights Array(Float64),
    p90_marker_positions Array(UInt64),
    p90_desired_marker_positions Array(Float64),
    p99_marker_heights Array(Float64),
    p99_marker_positions Array(UInt64),
    p99_desired_marker_positions Array(Float64)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_histogrami8_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, start_time, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS oximeter.measurements_histogrami8 ON CLUSTER oximeter_cluster
AS oximeter.measurements_histogrami8_local
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_histogrami8_local', xxHash64(splitByChar(':', timeseries_name)[1]));

CREATE TABLE IF NOT EXISTS oximeter.measurements_histogramu8_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    bins Array(UInt8),
    counts Array(UInt64),
    min UInt8,
    max UInt8,
    sum_of_samples Int64,
    squared_mean Float64,
    p50_marker_heights Array(Float64),
    p50_marker_positions Array(UInt64),
    p50_desired_marker_positions Array(Float64),
    p90_marker_heights Array(Float64),
    p90_marker_positions Array(UInt64),
    p90_desired_marker_positions Array(Float64),
    p99_marker_heights Array(Float64),
    p99_marker_positions Array(UInt64),
    p99_desired_marker_positions Array(Float64)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_histogramu8_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, start_time, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS oximeter.measurements_histogramu8 ON CLUSTER oximeter_cluster
AS oximeter.measurements_histogramu8_local
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_histogramu8_local', xxHash64(splitByChar(':', timeseries_name)[1]));

CREATE TABLE IF NOT EXISTS oximeter.measurements_histogrami16_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    bins Array(Int16),
    counts Array(UInt64),
    min Int16,
    max Int16,
    sum_of_samples Int64,
    squared_mean Float64,
    p50_marker_heights Array(Float64),
    p50_marker_positions Array(UInt64),
    p50_desired_marker_positions Array(Float64),
    p90_marker_heights Array(Float64),
    p90_marker_positions Array(UInt64),
    p90_desired_marker_positions Array(Float64),
    p99_marker_heights Array(Float64),
    p99_marker_positions Array(UInt64),
    p99_desired_marker_positions Array(Float64)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_histogrami16_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, start_time, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS oximeter.measurements_histogrami16 ON CLUSTER oximeter_cluster
AS oximeter.measurements_histogrami16_local
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_histogrami16_local', xxHash64(splitByChar(':', timeseries_name)[1]));

CREATE TABLE IF NOT EXISTS oximeter.measurements_histogramu16_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    bins Array(UInt16),
    counts Array(UInt64),
    min UInt16,
    max UInt16,
    sum_of_samples Int64,
    squared_mean Float64,
    p50_marker_heights Array(Float64),
    p50_marker_positions Array(UInt64),
    p50_desired_marker_positions Array(Float64),
    p90_marker_heights Array(Float64),
    p90_marker_positions Array(UInt64),
    p90_desired_marker_positions Array(Float64),
    p99_marker_heights Array(Float64),
    p99_marker_positions Array(UInt64),
    p99_desired_marker_positions Array(Float64)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_histogramu16_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, start_time, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS oximeter.measurements_histogramu16 ON CLUSTER oximeter_cluster
AS oximeter.measurements_histogramu16_local
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_histogramu16_local', xxHash64(splitByChar(':', timeseries_name)[1]));

CREATE TABLE IF NOT EXISTS oximeter.measurements_histogrami32_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    bins Array(Int32),
    counts Array(UInt64),
    min Int32,
    max Int32,
    sum_of_samples Int64,
    squared_mean Float64,
    p50_marker_heights Array(Float64),
    p50_marker_positions Array(UInt64),
    p50_desired_marker_positions Array(Float64),
    p90_marker_heights Array(Float64),
    p90_marker_positions Array(UInt64),
    p90_desired_marker_positions Array(Float64),
    p99_marker_heights Array(Float64),
    p99_marker_positions Array(UInt64),
    p99_desired_marker_positions Array(Float64)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_histogrami32_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, start_time, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS oximeter.measurements_histogrami32 ON CLUSTER oximeter_cluster
AS oximeter.measurements_histogrami32_local
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_histogrami32_local', xxHash64(splitByChar(':', timeseries_name)[1]));

CREATE TABLE IF NOT EXISTS oximeter.measurements_histogramu32_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    bins Array(UInt32),
    counts Array(UInt64),
    min UInt32,
    max UInt32,
    sum_of_samples Int64,
    squared_mean Float64,
    p50_marker_heights Array(Float64),
    p50_marker_positions Array(UInt64),
    p50_desired_marker_positions Array(Float64),
    p90_marker_heights Array(Float64),
    p90_marker_positions Array(UInt64),
    p90_desired_marker_positions Array(Float64),
    p99_marker_heights Array(Float64),
    p99_marker_positions Array(UInt64),
    p99_desired_marker_positions Array(Float64)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_histogramu32_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, start_time, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS oximeter.measurements_histogramu32 ON CLUSTER oximeter_cluster
AS oximeter.measurements_histogramu32_local
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_histogramu32_local', xxHash64(splitByChar(':', timeseries_name)[1]));

CREATE TABLE IF NOT EXISTS oximeter.measurements_histogrami64_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    bins Array(Int64),
    counts Array(UInt64),
    min Int64,
    max Int64,
    sum_of_samples Int64,
    squared_mean Float64,
    p50_marker_heights Array(Float64),
    p50_marker_positions Array(UInt64),
    p50_desired_marker_positions Array(Float64),
    p90_marker_heights Array(Float64),
    p90_marker_positions Array(UInt64),
    p90_desired_marker_positions Array(Float64),
    p99_marker_heights Array(Float64),
    p99_marker_positions Array(UInt64),
    p99_desired_marker_positions Array(Float64)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_histogrami64_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, start_time, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS oximeter.measurements_histogrami64 ON CLUSTER oximeter_cluster
AS oximeter.measurements_histogrami64_local
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_histogrami64_local', xxHash64(splitByChar(':', timeseries_name)[1]));

CREATE TABLE IF NOT EXISTS oximeter.measurements_histogramu64_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    bins Array(UInt64),
    counts Array(UInt64),
    min UInt64,
    max UInt64,
    sum_of_samples Int64,
    squared_mean Float64,
    p50_marker_heights Array(Float64),
    p50_marker_positions Array(UInt64),
    p50_desired_marker_positions Array(Float64),
    p90_marker_heights Array(Float64),
    p90_marker_positions Array(UInt64),
    p90_desired_marker_positions Array(Float64),
    p99_marker_heights Array(Float64),
    p99_marker_positions Array(UInt64),
    p99_desired_marker_positions Array(Float64)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_histogramu64_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, start_time, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS oximeter.measurements_histogramu64 ON CLUSTER oximeter_cluster
AS oximeter.measurements_histogramu64_local
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_histogramu64_local', xxHash64(splitByChar(':', timeseries_name)[1]));

CREATE TABLE IF NOT EXISTS oximeter.measurements_histogramf32_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    bins Array(Float32),
    counts Array(UInt64),
    min Float32,
    max Float32,
    sum_of_samples Float64,
    squared_mean Float64,
    p50_marker_heights Array(Float64),
    p50_marker_positions Array(UInt64),
    p50_desired_marker_positions Array(Float64),
    p90_marker_heights Array(Float64),
    p90_marker_positions Array(UInt64),
    p90_desired_marker_positions Array(Float64),
    p99_marker_heights Array(Float64),
    p99_marker_positions Array(UInt64),
    p99_desired_marker_positions Array(Float64)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_histogramf32_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, start_time, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS oximeter.measurements_histogramf32 ON CLUSTER oximeter_cluster
AS oximeter.measurements_histogramf32_local
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_histogramf32_local', xxHash64(splitByChar(':', timeseries_name)[1]));

CREATE TABLE IF NOT EXISTS oximeter.measurements_histogramf64_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    bins Array(Float64),
    counts Array(UInt64),
    min Float64,
    max Float64,
    sum_of_samples Float64,
    squared_mean Float64,
    p50_marker_heights Array(Float64),
    p50_marker_positions Array(UInt64),
    p50_desired_marker_positions Array(Float64),
    p90_marker_heights Array(Float64),
    p90_marker_positions Array(UInt64),
    p90_desired_marker_positions Array(Float64),
    p99_marker_heights Array(Float64),
    p99_marker_positions Array(UInt64),
    p99_desired_marker_positions Array(Float64)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_histogramf64_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, start_time, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS oximeter.measurements_histogramf64 ON CLUSTER oximeter_cluster
AS oximeter.measurements_histogramf64_local
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_histogramf64_local', xxHash64(splitByChar(':', timeseries_name)[1]));

/* The field tables store named dimensions of each timeseries.
 *
 * As with the measurement tables, there is one field table for each field data
 * type. Fields are deduplicated by using the "replacing merge tree", though
 * this behavior **must not** be relied upon for query correctness.
 *
 * The index for the fields differs from the measurements, however. Rows are
 * sorted by timeseries name, then field name, field value, and finally
 * timeseries key. This reflects the most common pattern for looking them up:
 * by field name and possibly value, within a timeseries. The resulting keys are
 * usually then used to look up measurements.
 *
 * NOTE: We may want to consider a secondary index on these tables, sorting by
 * timeseries name and then key, since it would improve lookups where one
 * already has the key. Realistically though, these tables are quite small and
 * so performance benefits will be low in absolute terms.
 */
CREATE TABLE IF NOT EXISTS oximeter.fields_bool_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    field_name String,
    field_value UInt8
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/fields_bool_local', '{replica}')
ORDER BY (timeseries_name, field_name, field_value, timeseries_key);

CREATE TABLE IF NOT EXISTS oximeter.fields_bool ON CLUSTER oximeter_cluster
AS oximeter.fields_bool_local
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'fields_bool_local', xxHash64(splitByChar(':', timeseries_name)[1]));

CREATE TABLE IF NOT EXISTS oximeter.fields_ipaddr_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    field_name String,
    field_value IPv6
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/fields_ipaddr_local', '{replica}')
ORDER BY (timeseries_name, field_name, field_value, timeseries_key);

CREATE TABLE IF NOT EXISTS oximeter.fields_ipaddr ON CLUSTER oximeter_cluster
AS oximeter.fields_ipaddr_local
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'fields_ipaddr_local', xxHash64(splitByChar(':', timeseries_name)[1]));

CREATE TABLE IF NOT EXISTS oximeter.fields_string_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    field_name String,
    field_value String
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/fields_string_local', '{replica}')
ORDER BY (timeseries_name, field_name, field_value, timeseries_key);

CREATE TABLE IF NOT EXISTS oximeter.fields_string ON CLUSTER oximeter_cluster
AS oximeter.fields_string_local
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'fields_string_local', xxHash64(splitByChar(':', timeseries_name)[1]));

CREATE TABLE IF NOT EXISTS oximeter.fields_i8_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    field_name String,
    field_value Int8
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/fields_i8_local', '{replica}')
ORDER BY (timeseries_name, field_name, field_value, timeseries_key);

CREATE TABLE IF NOT EXISTS oximeter.fields_i8 ON CLUSTER oximeter_cluster
AS oximeter.fields_i8_local
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'fields_i8_local', xxHash64(splitByChar(':', timeseries_name)[1]));

CREATE TABLE IF NOT EXISTS oximeter.fields_u8_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    field_name String,
    field_value UInt8
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/fields_u8_local', '{replica}')
ORDER BY (timeseries_name, field_name, field_value, timeseries_key);

CREATE TABLE IF NOT EXISTS oximeter.fields_u8 ON CLUSTER oximeter_cluster
AS oximeter.fields_u8_local
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'fields_u8_local', xxHash64(splitByChar(':', timeseries_name)[1]));

CREATE TABLE IF NOT EXISTS oximeter.fields_i16_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    field_name String,
    field_value Int16
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/fields_i16_local', '{replica}')
ORDER BY (timeseries_name, field_name, field_value, timeseries_key);

CREATE TABLE IF NOT EXISTS oximeter.fields_i16 ON CLUSTER oximeter_cluster
AS oximeter.fields_i16_local
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'fields_i16_local', xxHash64(splitByChar(':', timeseries_name)[1]));

CREATE TABLE IF NOT EXISTS oximeter.fields_u16_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    field_name String,
    field_value UInt16
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/fields_u16_local', '{replica}')
ORDER BY (timeseries_name, field_name, field_value, timeseries_key);

CREATE TABLE IF NOT EXISTS oximeter.fields_u16 ON CLUSTER oximeter_cluster
AS oximeter.fields_u16_local
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'fields_u16_local', xxHash64(splitByChar(':', timeseries_name)[1]));

CREATE TABLE IF NOT EXISTS oximeter.fields_i32_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    field_name String,
    field_value Int32
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/fields_i32_local', '{replica}')
ORDER BY (timeseries_name, field_name, field_value, timeseries_key);

CREATE TABLE IF NOT EXISTS oximeter.fields_i32 ON CLUSTER oximeter_cluster
AS oximeter.fields_i32_local
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'fields_i32_local', xxHash64(splitByChar(':', timeseries_name)[1]));

CREATE TABLE IF NOT EXISTS oximeter.fields_u32_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    field_name String,
    field_value UInt32
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/fields_u32_local', '{replica}')
ORDER BY (timeseries_name, field_name, field_value, timeseries_key);

CREATE TABLE IF NOT EXISTS oximeter.fields_u32 ON CLUSTER oximeter_cluster
AS oximeter.fields_u32_local
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'fields_u32_local', xxHash64(splitByChar(':', timeseries_name)[1]));

CREATE TABLE IF NOT EXISTS oximeter.fields_u64_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    field_name String,
    field_value UInt64
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/fields_u64_local', '{replica}')
ORDER BY (timeseries_name, field_name, field_value, timeseries_key);

CREATE TABLE IF NOT EXISTS oximeter.fields_u64 ON CLUSTER oximeter_cluster
AS oximeter.fields_u64_local
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'fields_u64_local', xxHash64(splitByChar(':', timeseries_name)[1]));
