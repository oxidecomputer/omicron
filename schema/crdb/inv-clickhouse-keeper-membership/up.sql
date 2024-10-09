CREATE TABLE IF NOT EXISTS omicron.public.inv_clickhouse_keeper_membership (
    inv_collection_id UUID NOT NULL,
    queried_keeper_id INT8 NOT NULL,
    leader_committed_log_index INT8 NOT NULL,
    raft_config INT8[] NOT NULL
);
