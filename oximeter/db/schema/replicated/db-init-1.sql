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

/* The version table contains metadata about the `oximeter` database */
CREATE TABLE IF NOT EXISTS oximeter.version ON CLUSTER oximeter_cluster
(
    value UInt64,
    timestamp DateTime64(9, 'UTC')
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/version', '{replica}')
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

CREATE TABLE IF NOT EXISTS oximeter.measurements_cumulativef64_local ON CLUSTER oximeter_cluster
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

CREATE TABLE IF NOT EXISTS oximeter.measurements_cumulativef64 ON CLUSTER oximeter_cluster
AS oximeter.measurements_cumulativef64_local
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_cumulativef64_local', xxHash64(splitByChar(':', timeseries_name)[1]));

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
CREATE TABLE IF NOT EXISTS oximeter.fields_i64_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    field_name String,
    field_value Int64,
    last_updated_at DateTime MATERIALIZED now()
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/fields_i64_local', '{replica}')
ORDER BY (timeseries_name, field_name, field_value, timeseries_key)
TTL last_updated_at + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS oximeter.fields_i64 ON CLUSTER oximeter_cluster
AS oximeter.fields_i64_local
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'fields_i64_local', xxHash64(splitByChar(':', timeseries_name)[1]));


CREATE TABLE IF NOT EXISTS oximeter.fields_uuid_local ON CLUSTER oximeter_cluster
(
    timeseries_name String,
    timeseries_key UInt64,
    field_name String,
    field_value UUID,
    last_updated_at DateTime MATERIALIZED now()
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/fields_uuid_local', '{replica}')
ORDER BY (timeseries_name, field_name, field_value, timeseries_key)
TTL last_updated_at + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS oximeter.fields_uuid ON CLUSTER oximeter_cluster
AS oximeter.fields_uuid_local
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'fields_uuid_local', xxHash64(splitByChar(':', timeseries_name)[1]));

/* The timeseries schema table stores the extracted schema for the samples
 * oximeter collects.
 */
CREATE TABLE IF NOT EXISTS oximeter.timeseries_schema_local ON CLUSTER oximeter_cluster
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
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/timeseries_schema_local', '{replica}')
ORDER BY (timeseries_name, fields.name);

CREATE TABLE IF NOT EXISTS oximeter.timeseries_schema ON CLUSTER oximeter_cluster
AS oximeter.timeseries_schema_local
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'timeseries_schema_local', xxHash64(splitByChar(':', timeseries_name)[1]));
