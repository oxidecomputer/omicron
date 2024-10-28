CREATE TABLE IF NOT EXISTS omicron.public.clickhouse_policy (
    version INT8 PRIMARY KEY,
    clickhouse_mode omicron.public.clickhouse_mode NOT NULL,
    clickhouse_cluster_target_servers INT2 NOT NULL,
    clickhouse_cluster_target_keepers INT2 NOT NULL,
    time_created TIMESTAMPTZ NOT NULL
);
