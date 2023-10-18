CREATE DATABASE IF NOT EXISTS oximeter ON CLUSTER oximeter_cluster;
--
CREATE TABLE IF NOT EXISTS oximeter.version ON CLUSTER oximeter_cluster
(
    value UInt64,
    timestamp DateTime64(9, 'UTC')
)
ENGINE = ReplicatedMergeTree()
ORDER BY (value, timestamp);
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
CREATE TABLE IF NOT EXISTS oximeter.measurements_i8_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum Int8
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_i8_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_i8 ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum Int8
)
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_i8_local', xxHash64(splitByChar(':', timeseries_name)[1]));
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_u8_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum UInt8
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_u8_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_u8 ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum UInt8
)
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_u8_local', xxHash64(splitByChar(':', timeseries_name)[1]));
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_i16_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum Int16
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_i16_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_i16 ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum Int16
)
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_i16_local', xxHash64(splitByChar(':', timeseries_name)[1]));
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_u16_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum UInt16
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_u16_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_u16 ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum UInt16
)
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_u16_local', xxHash64(splitByChar(':', timeseries_name)[1]));
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_i32_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum Int32
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_i32_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_i32 ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum Int32
)
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_i32_local', xxHash64(splitByChar(':', timeseries_name)[1]));
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_u32_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum UInt32
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_u32_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_u32 ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum UInt32
)
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_u32_local', xxHash64(splitByChar(':', timeseries_name)[1]));
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
CREATE TABLE IF NOT EXISTS oximeter.measurements_u64_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum UInt64
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_u64_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_u64 ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum UInt64
)
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_u64_local', xxHash64(splitByChar(':', timeseries_name)[1]));
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
CREATE TABLE IF NOT EXISTS oximeter.measurements_cumulativeu64_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    datum UInt64
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_cumulativeu64_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, start_time, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_cumulativeu64 ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    datum UInt64
)
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_cumulativeu64_local', xxHash64(splitByChar(':', timeseries_name)[1]));
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_cumulativef32_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    datum Float32
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_cumulativef32_local', '{replica}')
ORDER BY (timeseries_name, timeseries_key, start_time, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_cumulativef32 ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    datum Float32
)
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_cumulativef32_local', xxHash64(splitByChar(':', timeseries_name)[1]));
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
--
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
--
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
--
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
--
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
--
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
--
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
--
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
--
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
--
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
--
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
--
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
--
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
--
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
--
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
