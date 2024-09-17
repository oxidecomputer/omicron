CREATE TABLE IF NOT EXISTS omicron.public.bp_clickhouse_server_zone_id_to_node_id (
    blueprint_id UUID NOT NULL,
    omicron_zone_id UUID NOT NULL,
    server_id INT8 NOT NULL,
    PRIMARY KEY (blueprint_id, omicron_zone_id, keeper_node_id)
)
