CREATE DATABASE IF NOT EXISTS oximeter ON CLUSTER oximeter_cluster;

/* The version table contains metadata about the `oximeter` database */
CREATE TABLE IF NOT EXISTS oximeter.version ON CLUSTER oximeter_cluster
(
    value UInt64,
    timestamp DateTime64(9, 'UTC')
)
ENGINE = ReplicatedMergeTree()
ORDER BY (value, timestamp);

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
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum Nullable(UInt8)
)
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
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum Nullable(Int8)
)
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
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum Nullable(UInt8)
)
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
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum Nullable(Int16)
)
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
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum Nullable(UInt16)
)
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
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum Nullable(Int32)
)
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
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum Nullable(UInt32)
)
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
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum Nullable(Int64)
)
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
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum Nullable(UInt64)
)
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
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum Nullable(Float32)
)
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
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum Nullable(Float64)
)
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
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum Nullable(String)
)
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
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum Array(UInt8)
)
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
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    datum Nullable(Int64)
)
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
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    datum Nullable(UInt64)
)
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
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    datum Nullable(Float32)
)
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_cumulativef32_local', xxHash64(splitByChar(':', timeseries_name)[1]));

CREATE TABLE IF NOT EXISTS oximeter.measurements_cumulativef64_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    datum Nullable(Float64)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_cumulativef64_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, start_time, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS oximeter.measurements_cumulativef64 ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    datum Nullable(Float64)
)
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_cumulativef64_local', xxHash64(splitByChar(':', timeseries_name)[1]));

CREATE TABLE IF NOT EXISTS oximeter.measurements_histogrami8_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    bins Array(Int8),
    counts Array(UInt64)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_histogrami8_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, start_time, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS oximeter.measurements_histogrami8 ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    bins Array(Int8),
    counts Array(UInt64)
)
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_histogrami8_local', xxHash64(splitByChar(':', timeseries_name)[1]));

CREATE TABLE IF NOT EXISTS oximeter.measurements_histogramu8_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    bins Array(UInt8),
    counts Array(UInt64)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_histogramu8_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, start_time, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS oximeter.measurements_histogramu8 ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    bins Array(UInt8),
    counts Array(UInt64)
)
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_histogramu8_local', xxHash64(splitByChar(':', timeseries_name)[1]));

CREATE TABLE IF NOT EXISTS oximeter.measurements_histogrami16_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    bins Array(Int16),
    counts Array(UInt64)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_histogrami16_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, start_time, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS oximeter.measurements_histogrami16 ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    bins Array(Int16),
    counts Array(UInt64)
)
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_histogrami16_local', xxHash64(splitByChar(':', timeseries_name)[1]));

CREATE TABLE IF NOT EXISTS oximeter.measurements_histogramu16_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    bins Array(UInt16),
    counts Array(UInt64)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_histogramu16_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, start_time, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS oximeter.measurements_histogramu16 ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    bins Array(UInt16),
    counts Array(UInt64)
)
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_histogramu16_local', xxHash64(splitByChar(':', timeseries_name)[1]));

CREATE TABLE IF NOT EXISTS oximeter.measurements_histogrami32_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    bins Array(Int32),
    counts Array(UInt64)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_histogrami32_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, start_time, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS oximeter.measurements_histogrami32 ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    bins Array(Int32),
    counts Array(UInt64)
)
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_histogrami32_local', xxHash64(splitByChar(':', timeseries_name)[1]));

CREATE TABLE IF NOT EXISTS oximeter.measurements_histogramu32_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    bins Array(UInt32),
    counts Array(UInt64)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_histogramu32_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, start_time, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS oximeter.measurements_histogramu32 ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    bins Array(UInt32),
    counts Array(UInt64)
)
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_histogramu32_local', xxHash64(splitByChar(':', timeseries_name)[1]));

CREATE TABLE IF NOT EXISTS oximeter.measurements_histogrami64_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    bins Array(Int64),
    counts Array(UInt64)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_histogrami64_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, start_time, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS oximeter.measurements_histogrami64 ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    bins Array(Int64),
    counts Array(UInt64)
)
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_histogrami64_local', xxHash64(splitByChar(':', timeseries_name)[1]));

CREATE TABLE IF NOT EXISTS oximeter.measurements_histogramu64_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    bins Array(UInt64),
    counts Array(UInt64)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_histogramu64_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, start_time, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS oximeter.measurements_histogramu64 ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    bins Array(UInt64),
    counts Array(UInt64)
)
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_histogramu64_local', xxHash64(splitByChar(':', timeseries_name)[1]));

CREATE TABLE IF NOT EXISTS oximeter.measurements_histogramf32_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    bins Array(Float32),
    counts Array(UInt64)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_histogramf32_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, start_time, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS oximeter.measurements_histogramf32 ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    bins Array(Float32),
    counts Array(UInt64)
)
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_histogramf32_local', xxHash64(splitByChar(':', timeseries_name)[1]));

CREATE TABLE IF NOT EXISTS oximeter.measurements_histogramf64_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    bins Array(Float64),
    counts Array(UInt64)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_histogramf64_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, start_time, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS oximeter.measurements_histogramf64 ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    bins Array(Float64),
    counts Array(UInt64)
)
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
CREATE TABLE IF NOT EXISTS oximeter.fields_bool ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    field_name String,
    field_value UInt8
)
ENGINE = ReplicatedReplacingMergeTree()
ORDER BY (timeseries_name, field_name, field_value, timeseries_key);

CREATE TABLE IF NOT EXISTS oximeter.fields_i8 ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    field_name String,
    field_value Int8
)
ENGINE = ReplicatedReplacingMergeTree()
ORDER BY (timeseries_name, field_name, field_value, timeseries_key);

CREATE TABLE IF NOT EXISTS oximeter.fields_u8 ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    field_name String,
    field_value UInt8
)
ENGINE = ReplicatedReplacingMergeTree()
ORDER BY (timeseries_name, field_name, field_value, timeseries_key);

CREATE TABLE IF NOT EXISTS oximeter.fields_i16 ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    field_name String,
    field_value Int16
)
ENGINE = ReplicatedReplacingMergeTree()
ORDER BY (timeseries_name, field_name, field_value, timeseries_key);

CREATE TABLE IF NOT EXISTS oximeter.fields_u16 ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    field_name String,
    field_value UInt16
)
ENGINE = ReplicatedReplacingMergeTree()
ORDER BY (timeseries_name, field_name, field_value, timeseries_key);

CREATE TABLE IF NOT EXISTS oximeter.fields_i32 ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    field_name String,
    field_value Int32
)
ENGINE = ReplicatedReplacingMergeTree()
ORDER BY (timeseries_name, field_name, field_value, timeseries_key);

CREATE TABLE IF NOT EXISTS oximeter.fields_u32 ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    field_name String,
    field_value UInt32
)
ENGINE = ReplicatedReplacingMergeTree()
ORDER BY (timeseries_name, field_name, field_value, timeseries_key);

CREATE TABLE IF NOT EXISTS oximeter.fields_i64 ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    field_name String,
    field_value Int64
)
ENGINE = ReplicatedReplacingMergeTree()
ORDER BY (timeseries_name, field_name, field_value, timeseries_key);

CREATE TABLE IF NOT EXISTS oximeter.fields_u64 ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    field_name String,
    field_value UInt64
)
ENGINE = ReplicatedReplacingMergeTree()
ORDER BY (timeseries_name, field_name, field_value, timeseries_key);

CREATE TABLE IF NOT EXISTS oximeter.fields_ipaddr ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    field_name String,
    field_value IPv6
)
ENGINE = ReplicatedReplacingMergeTree()
ORDER BY (timeseries_name, field_name, field_value, timeseries_key);

CREATE TABLE IF NOT EXISTS oximeter.fields_string ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    field_name String,
    field_value String
)
ENGINE = ReplicatedReplacingMergeTree()
ORDER BY (timeseries_name, field_name, field_value, timeseries_key);

CREATE TABLE IF NOT EXISTS oximeter.fields_uuid ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    field_name String,
    field_value UUID
)
ENGINE = ReplicatedReplacingMergeTree()
ORDER BY (timeseries_name, field_name, field_value, timeseries_key);

/* The timeseries schema table stores the extracted schema for the samples
 * oximeter collects.
 */
CREATE TABLE IF NOT EXISTS oximeter.timeseries_schema ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    fields Nested(
        name String,
        type Enum(
            'Bool' = 1,
            'I64' = 2,
            'IpAddr' = 3,
            'String' = 4,
            'Uuid' = 6,
            'I8' = 7,
            'U8' = 8,
            'I16' = 9,
            'U16' = 10,
            'I32' = 11,
            'U32' = 12,
            'U64' = 13
        ),
        source Enum(
            'Target' = 1,
            'Metric' = 2
        )
    ),
    datum_type Enum(
        'Bool' = 1,
        'I64' = 2,
        'F64' = 3,
        'String' = 4,
        'Bytes' = 5,
        'CumulativeI64' = 6,
        'CumulativeF64' = 7,
        'HistogramI64' = 8,
        'HistogramF64' = 9,
        'I8' = 10,
        'U8' = 11,
        'I16' = 12,
        'U16' = 13,
        'I32' = 14,
        'U32' = 15,
        'U64' = 16,
        'F32' = 17,
        'CumulativeU64' = 18,
        'CumulativeF32' = 19,
        'HistogramI8' = 20,
        'HistogramU8' = 21,
        'HistogramI16' = 22,
        'HistogramU16' = 23,
        'HistogramI32' = 24,
        'HistogramU32' = 25,
        'HistogramU64' = 26,
        'HistogramF32' = 27
    ),
    created DateTime64(9, 'UTC')
)
ENGINE = ReplicatedMergeTree()
ORDER BY (timeseries_name, fields.name);
