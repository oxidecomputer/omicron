CREATE TABLE IF NOT EXISTS oximeter.measurements_cumulativef64_local_new_15 ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    start_time DateTime64(9, 'UTC'),
    timestamp DateTime64(9, 'UTC'),
    datum Nullable(Float64)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/measurements_cumulativef64_local', '{replica}')
PARTITION BY (toYYYYMMDD(timestamp))
ORDER BY (timeseries_name, timeseries_key, start_time, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

INSERT INTO oximeter.measurements_cumulativef64_local_new_15
SELECT * FROM oximeter.measurements_cumulativef64_local;

RENAME TABLE
    oximeter.measurements_cumulativef64_local_new_15 to oximeter.measurements.cumulativef64_local,
    oximeter.measurements_cumulativef64_local to oximeter.measurements.cumulativef64_local_old_15;

-- TODO: Drop the original table in a separate step, after verifying the migration.
-- DROP TABLE oximeter.measurements_cumulativef64_old_15;
