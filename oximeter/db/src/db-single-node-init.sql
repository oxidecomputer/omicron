CREATE DATABASE IF NOT EXISTS oximeter;
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_bool
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum UInt8
)
ENGINE = MergeTree()
ORDER BY (timeseries_name, timeseries_key, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_i8
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum Int8
)
ENGINE = MergeTree()
ORDER BY (timeseries_name, timeseries_key, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_u8
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum UInt8
)
ENGINE = MergeTree()
ORDER BY (timeseries_name, timeseries_key, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_i16
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum Int16
)
ENGINE = MergeTree()
ORDER BY (timeseries_name, timeseries_key, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_u16
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum UInt16
)
ENGINE = MergeTree()
ORDER BY (timeseries_name, timeseries_key, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_i32
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum Int32
)
ENGINE = MergeTree()
ORDER BY (timeseries_name, timeseries_key, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_u32
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum UInt32
)
ENGINE = MergeTree()
ORDER BY (timeseries_name, timeseries_key, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_i64
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum Int64
)
ENGINE = MergeTree()
ORDER BY (timeseries_name, timeseries_key, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_u64
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum UInt64
)
ENGINE = MergeTree()
ORDER BY (timeseries_name, timeseries_key, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_f32
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum Float32
)
ENGINE = MergeTree()
ORDER BY (timeseries_name, timeseries_key, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_f64
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum Float64
)
ENGINE = MergeTree()
ORDER BY (timeseries_name, timeseries_key, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_string
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum String
)
ENGINE = MergeTree()
ORDER BY (timeseries_name, timeseries_key, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_bytes
(
    timeseries_name String,
    timeseries_key UInt64,
    timestamp DateTime64(9, 'UTC'),
    datum Array(UInt8)
)
ENGINE = MergeTree()
ORDER BY (timeseries_name, timeseries_key, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_cumulativei64
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    datum Int64
)
ENGINE = MergeTree()
ORDER BY (timeseries_name, timeseries_key, start_time, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_cumulativeu64
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    datum UInt64
)
ENGINE = MergeTree()
ORDER BY (timeseries_name, timeseries_key, start_time, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_cumulativef32
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    datum Float32
)
ENGINE = MergeTree()
ORDER BY (timeseries_name, timeseries_key, start_time, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;
--
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_cumulativef64
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    datum Float64
)
ENGINE = MergeTree()
ORDER BY (timeseries_name, timeseries_key, start_time, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_histogrami8
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    bins Array(Int8),
    counts Array(UInt64)
)
ENGINE = MergeTree()
ORDER BY (timeseries_name, timeseries_key, start_time, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_histogramu8
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    bins Array(UInt8),
    counts Array(UInt64)
)
ENGINE = MergeTree()
ORDER BY (timeseries_name, timeseries_key, start_time, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_histogrami16
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    bins Array(Int16),
    counts Array(UInt64)
)
ENGINE = MergeTree()
ORDER BY (timeseries_name, timeseries_key, start_time, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_histogramu16
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    bins Array(UInt16),
    counts Array(UInt64)
)
ENGINE = MergeTree()
ORDER BY (timeseries_name, timeseries_key, start_time, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_histogrami32
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    bins Array(Int32),
    counts Array(UInt64)
)
ENGINE = MergeTree()
ORDER BY (timeseries_name, timeseries_key, start_time, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_histogramu32
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    bins Array(UInt32),
    counts Array(UInt64)
)
ENGINE = MergeTree()
ORDER BY (timeseries_name, timeseries_key, start_time, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_histogrami64
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    bins Array(Int64),
    counts Array(UInt64)
)
ENGINE = MergeTree()
ORDER BY (timeseries_name, timeseries_key, start_time, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_histogramu64
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    bins Array(UInt64),
    counts Array(UInt64)
)
ENGINE = MergeTree()
ORDER BY (timeseries_name, timeseries_key, start_time, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_histogramf32
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    bins Array(Float32),
    counts Array(UInt64)
)
ENGINE = MergeTree()
ORDER BY (timeseries_name, timeseries_key, start_time, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_histogramf64
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    bins Array(Float64),
    counts Array(UInt64)
)
ENGINE = MergeTree()
ORDER BY (timeseries_name, timeseries_key, start_time, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;
--
CREATE TABLE IF NOT EXISTS oximeter.fields_bool
(
    timeseries_name String,
    timeseries_key UInt64,
    field_name String,
    field_value UInt8
)
ENGINE = ReplacingMergeTree()
ORDER BY (timeseries_name, field_name, field_value, timeseries_key);
--
CREATE TABLE IF NOT EXISTS oximeter.fields_i8
(
    timeseries_name String,
    timeseries_key UInt64,
    field_name String,
    field_value Int8
)
ENGINE = ReplacingMergeTree()
ORDER BY (timeseries_name, field_name, field_value, timeseries_key);
--
CREATE TABLE IF NOT EXISTS oximeter.fields_u8
(
    timeseries_name String,
    timeseries_key UInt64,
    field_name String,
    field_value UInt8
)
ENGINE = ReplacingMergeTree()
ORDER BY (timeseries_name, field_name, field_value, timeseries_key);
--
CREATE TABLE IF NOT EXISTS oximeter.fields_i16
(
    timeseries_name String,
    timeseries_key UInt64,
    field_name String,
    field_value Int16
)
ENGINE = ReplacingMergeTree()
ORDER BY (timeseries_name, field_name, field_value, timeseries_key);
--
CREATE TABLE IF NOT EXISTS oximeter.fields_u16
(
    timeseries_name String,
    timeseries_key UInt64,
    field_name String,
    field_value UInt16
)
ENGINE = ReplacingMergeTree()
ORDER BY (timeseries_name, field_name, field_value, timeseries_key);
--
CREATE TABLE IF NOT EXISTS oximeter.fields_i32
(
    timeseries_name String,
    timeseries_key UInt64,
    field_name String,
    field_value Int32
)
ENGINE = ReplacingMergeTree()
ORDER BY (timeseries_name, field_name, field_value, timeseries_key);
--
CREATE TABLE IF NOT EXISTS oximeter.fields_u32
(
    timeseries_name String,
    timeseries_key UInt64,
    field_name String,
    field_value UInt32
)
ENGINE = ReplacingMergeTree()
ORDER BY (timeseries_name, field_name, field_value, timeseries_key);
--
CREATE TABLE IF NOT EXISTS oximeter.fields_i64
(
    timeseries_name String,
    timeseries_key UInt64,
    field_name String,
    field_value Int64
)
ENGINE = ReplacingMergeTree()
ORDER BY (timeseries_name, field_name, field_value, timeseries_key);
--
CREATE TABLE IF NOT EXISTS oximeter.fields_u64
(
    timeseries_name String,
    timeseries_key UInt64,
    field_name String,
    field_value UInt64
)
ENGINE = ReplacingMergeTree()
ORDER BY (timeseries_name, field_name, field_value, timeseries_key);
--
CREATE TABLE IF NOT EXISTS oximeter.fields_ipaddr
(
    timeseries_name String,
    timeseries_key UInt64,
    field_name String,
    field_value IPv6
)
ENGINE = ReplacingMergeTree()
ORDER BY (timeseries_name, field_name, field_value, timeseries_key);
--
CREATE TABLE IF NOT EXISTS oximeter.fields_string
(
    timeseries_name String,
    timeseries_key UInt64,
    field_name String,
    field_value String
)
ENGINE = ReplacingMergeTree()
ORDER BY (timeseries_name, field_name, field_value, timeseries_key);
--
CREATE TABLE IF NOT EXISTS oximeter.fields_uuid
(
    timeseries_name String,
    timeseries_key UInt64,
    field_name String,
    field_value UUID
)
ENGINE = ReplacingMergeTree()
ORDER BY (timeseries_name, field_name, field_value, timeseries_key);
--
CREATE TABLE IF NOT EXISTS oximeter.timeseries_schema
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
ENGINE = MergeTree()
ORDER BY (timeseries_name, fields.name);
