CREATE TABLE IF NOT EXISTS omicron.public.bp_clickhouse_cluster_config (
    blueprint_id UUID PRIMARY KEY,
    generation INT8 NOT NULL,
    max_used_server_id INT8 NOT NULL,
    max_used_keeper_id INT8 NOT NULL,
    cluster_name TEXT NOT NULL,
    cluster_secret TEXT NOT NULL,
    highest_seen_keeper_leader_committed_log_index INT8 NOT NULL
);
