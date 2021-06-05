CREATE DATABASE IF NOT EXISTS oximeter;
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_bool
(
    target_name String,
    metric_name String,
    timeseries_key String,
    timestamp DateTime64(6, 'UTC'),
    value UInt8
)
ENGINE = MergeTree()
PRIMARY KEY (target_name, metric_name, timeseries_key)
ORDER BY (target_name, metric_name, timeseries_key, timestamp);
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_i64
(
    target_name String,
    metric_name String,
    timeseries_key String,
    timestamp DateTime64(6, 'UTC'),
    value Int64
)
ENGINE = MergeTree()
PRIMARY KEY (target_name, metric_name, timeseries_key)
ORDER BY (target_name, metric_name, timeseries_key, timestamp);
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_f64
(
    target_name String,
    metric_name String,
    timeseries_key String,
    timestamp DateTime64(6, 'UTC'),
    value Float64
)
ENGINE = MergeTree()
PRIMARY KEY (target_name, metric_name, timeseries_key)
ORDER BY (target_name, metric_name, timeseries_key, timestamp);
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_string
(
    target_name String,
    metric_name String,
    timeseries_key String,
    timestamp DateTime64(6, 'UTC'),
    value String
)
ENGINE = MergeTree()
PRIMARY KEY (target_name, metric_name, timeseries_key)
ORDER BY (target_name, metric_name, timeseries_key, timestamp);
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_bytes
(
    target_name String,
    metric_name String,
    timeseries_key String,
    timestamp DateTime64(6, 'UTC'),
    value Array(UInt8)
)
ENGINE = MergeTree()
PRIMARY KEY (target_name, metric_name, timeseries_key)
ORDER BY (target_name, metric_name, timeseries_key, timestamp);
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_cumulativei64
(
    target_name String,
    metric_name String,
    timeseries_key String,
    timestamp DateTime64(6, 'UTC'),
    value Int64
)
ENGINE = MergeTree()
PRIMARY KEY (target_name, metric_name, timeseries_key)
ORDER BY (target_name, metric_name, timeseries_key, timestamp);
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_cumulativef64
(
    target_name String,
    metric_name String,
    timeseries_key String,
    timestamp DateTime64(6, 'UTC'),
    value Float64
)
ENGINE = MergeTree()
PRIMARY KEY (target_name, metric_name, timeseries_key)
ORDER BY (target_name, metric_name, timeseries_key, timestamp);
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_histogrami64
(
    target_name String,
    metric_name String,
    timeseries_key String,
    timestamp DateTime64(6, 'UTC'),
    bins Array(Int64),
    counts Array(UInt64)
)
ENGINE = MergeTree()
PRIMARY KEY (target_name, metric_name, timeseries_key)
ORDER BY (target_name, metric_name, timeseries_key, timestamp);
--
CREATE TABLE IF NOT EXISTS oximeter.measurements_histogramf64
(
    target_name String,
    metric_name String,
    timeseries_key String,
    timestamp DateTime64(6, 'UTC'),
    bins Array(Float64),
    counts Array(UInt64)
)
ENGINE = MergeTree()
PRIMARY KEY (target_name, metric_name, timeseries_key)
ORDER BY (target_name, metric_name, timeseries_key, timestamp);
--
CREATE TABLE IF NOT EXISTS oximeter.metric_schema
(
    metric_name String,
    fields Nested(
        name String,
        type Enum(
            'Bool' = 1,
            'I64' = 2,
            'IpAddr' = 3,
            'String' = 4,
            'Bytes' = 5,
            'Uuid' = 6
    )),
    measurement_type Enum(
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
    created DateTime64(6, 'UTC')
)
ENGINE = MergeTree()
PRIMARY KEY (metric_name, fields.name);
--
CREATE TABLE IF NOT EXISTS oximeter.metric_fields_bool
(
    metric_name String,
    timeseries_key String,
    field_name String,
    field_value UInt8,
    timestamp DateTime64(6, 'UTC')
)
ENGINE = MergeTree()
PRIMARY KEY (metric_name, field_name, timeseries_key)
ORDER BY (metric_name, field_name, timeseries_key, timestamp);
--
CREATE TABLE IF NOT EXISTS oximeter.metric_fields_i64
(
    metric_name String,
    timeseries_key String,
    field_name String,
    field_value Int64,
    timestamp DateTime64(6, 'UTC')
)
ENGINE = MergeTree()
PRIMARY KEY (metric_name, field_name, timeseries_key)
ORDER BY (metric_name, field_name, timeseries_key, timestamp);
--
CREATE TABLE IF NOT EXISTS oximeter.metric_fields_ipaddr
(
    metric_name String,
    timeseries_key String,
    field_name String,
    field_value IPv6,
    timestamp DateTime64(6, 'UTC')
)
ENGINE = MergeTree()
PRIMARY KEY (metric_name, field_name, timeseries_key)
ORDER BY (metric_name, field_name, timeseries_key, timestamp);
--
CREATE TABLE IF NOT EXISTS oximeter.metric_fields_string
(
    metric_name String,
    timeseries_key String,
    field_name String,
    field_value String,
    timestamp DateTime64(6, 'UTC')
)
ENGINE = MergeTree()
PRIMARY KEY (metric_name, field_name, timeseries_key)
ORDER BY (metric_name, field_name, timeseries_key, timestamp);
--
CREATE TABLE IF NOT EXISTS oximeter.metric_fields_uuid
(
    metric_name String,
    timeseries_key String,
    field_name String,
    field_value UUID,
    timestamp DateTime64(6, 'UTC')
)
ENGINE = MergeTree()
PRIMARY KEY (metric_name, field_name, timeseries_key)
ORDER BY (metric_name, field_name, timeseries_key, timestamp);
--
CREATE TABLE IF NOT EXISTS oximeter.target_schema
(
    target_name String,
    fields Nested(
        name String,
        type Enum(
            'Bool' = 1,
            'I64' = 2,
            'IpAddr' = 3,
            'String' = 4,
            'Bytes' = 5,
            'Uuid' = 6
    )),
    created DateTime64(6, 'UTC')
)
ENGINE = MergeTree()
PRIMARY KEY (target_name, fields.name);
--
CREATE TABLE IF NOT EXISTS oximeter.target_fields_bool
(
    target_name String,
    timeseries_key String,
    field_name String,
    field_value UInt8,
    timestamp DateTime64(6, 'UTC')
)
ENGINE = MergeTree()
PRIMARY KEY (target_name, field_name, timeseries_key)
ORDER BY (target_name, field_name, timeseries_key, timestamp);
--
CREATE TABLE IF NOT EXISTS oximeter.target_fields_i64
(
    target_name String,
    timeseries_key String,
    field_name String,
    field_value Int64,
    timestamp DateTime64(6, 'UTC')
)
ENGINE = MergeTree()
PRIMARY KEY (target_name, field_name, timeseries_key)
ORDER BY (target_name, field_name, timeseries_key, timestamp);
--
CREATE TABLE IF NOT EXISTS oximeter.target_fields_ipaddr
(
    target_name String,
    timeseries_key String,
    field_name String,
    field_value IPv6,
    timestamp DateTime64(6, 'UTC')
)
ENGINE = MergeTree()
PRIMARY KEY (target_name, field_name, timeseries_key)
ORDER BY (target_name, field_name, timeseries_key, timestamp);
--
CREATE TABLE IF NOT EXISTS oximeter.target_fields_string
(
    target_name String,
    timeseries_key String,
    field_name String,
    field_value String,
    timestamp DateTime64(6, 'UTC')
)
ENGINE = MergeTree()
PRIMARY KEY (target_name, field_name, timeseries_key)
ORDER BY (target_name, field_name, timeseries_key, timestamp);
--
CREATE TABLE IF NOT EXISTS oximeter.target_fields_uuid
(
    target_name String,
    timeseries_key String,
    field_name String,
    field_value UUID,
    timestamp DateTime64(6, 'UTC')
)
ENGINE = MergeTree()
PRIMARY KEY (target_name, field_name, timeseries_key)
ORDER BY (target_name, field_name, timeseries_key, timestamp);
