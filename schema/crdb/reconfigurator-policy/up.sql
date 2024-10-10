CREATE TABLE IF NOT EXISTS omicron.public.reconfigurator_policy (
    version INT8 PRIMARY KEY,
    clickhouse_cluster_enabled BOOL NOT NULL
    clickhouse_single_node_enabled BOOL NOT NULL
    time_created TIMESTAMPTZ NOT NULL,
);
