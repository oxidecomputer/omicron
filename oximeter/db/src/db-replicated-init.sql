CREATE DATABASE IF NOT EXISTS oximeter ON CLUSTER oximeter_cluster;
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_bool_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum UInt8
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_bool_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_bool ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum UInt8
)
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_bool_local', xxHash64(splitByChar(':', timeseries_name)[1]));
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_i64_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum Int64
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_i64_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_i64 ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum Int64
)
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_i64_local', xxHash64(splitByChar(':', timeseries_name)[1]));
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_f64_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum Float64
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_f64_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_f64 ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum Float64
)
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_f64_local', xxHash64(splitByChar(':', timeseries_name)[1]));
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_string_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_string_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_string ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum String
)
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_string_local', xxHash64(splitByChar(':', timeseries_name)[1]));
--
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
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_bytes ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum Array(UInt8)
)
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_bytes_local', xxHash64(splitByChar(':', timeseries_name)[1]));
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_cumulativei64_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    datum Int64
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_cumulativei64_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, start_time, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_cumulativei64 ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    datum Int64
)
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_cumulativei64_local', xxHash64(splitByChar(':', timeseries_name)[1]));
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_cumulativef64_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    datum Float64
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_cumulativef64_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, start_time, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_cumulativef64 ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    datum Float64
)
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_cumulativef64_local', xxHash64(splitByChar(':', timeseries_name)[1]));
--
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
--
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
--
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
--
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
--
CREATE TABLE IF NOT EXISTS oximeter.fields_bool ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    field_name String,
    field_value UInt8
)
ENGINE = ReplicatedReplacingMergeTree()
ORDER BY (timeseries_name, field_name, field_value, timeseries_key);
--
CREATE TABLE IF NOT EXISTS oximeter.fields_i64 ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    field_name String,
    field_value Int64
)
ENGINE = ReplicatedReplacingMergeTree()
ORDER BY (timeseries_name, field_name, field_value, timeseries_key);
--
CREATE TABLE IF NOT EXISTS oximeter.fields_ipaddr ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    field_name String,
    field_value IPv6
)
ENGINE = ReplicatedReplacingMergeTree()
ORDER BY (timeseries_name, field_name, field_value, timeseries_key);
--
CREATE TABLE IF NOT EXISTS oximeter.fields_string ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    field_name String,
    field_value String
)
ENGINE = ReplicatedReplacingMergeTree()
ORDER BY (timeseries_name, field_name, field_value, timeseries_key);
--
CREATE TABLE IF NOT EXISTS oximeter.fields_uuid ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    field_name String,
    field_value UUID
)
ENGINE = ReplicatedReplacingMergeTree()
ORDER BY (timeseries_name, field_name, field_value, timeseries_key);
--
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
            'Uuid' = 6
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
        'HistogramF64' = 9
    ),
    created DateTime64(9, 'UTC')
)
ENGINE = ReplicatedMergeTree()
ORDER BY (timeseries_name, fields.name);
