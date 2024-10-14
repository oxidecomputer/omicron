CREATE TABLE IF NOT EXISTS omicron.public.clickhouse_policy (
    version INT8 PRIMARY KEY,
    clickhouse_cluster_enabled BOOL NOT NULL
    clickhouse_single_node_enabled BOOL NOT NULL
    clickhouse_cluster_target_servers INT2 NOT NULL
    clickhouse_cluster_target_keepers INT2 NOT NULL
    time_created TIMESTAMPTZ NOT NULL,
);
