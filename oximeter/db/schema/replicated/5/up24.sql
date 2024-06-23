CREATE TABLE IF NOT EXISTS oximeter.measurements_histogramu32 ON CLUSTER oximeter_cluster
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
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_histogramu32_local', xxHash64(splitByChar(':', timeseries_name)[1]));
