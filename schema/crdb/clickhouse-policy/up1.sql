CREATE TYPE IF NOT EXISTS omicron.public.clickhouse_mode AS ENUM (
   'single_node_only',
   'cluster_only',
   'both'
);
