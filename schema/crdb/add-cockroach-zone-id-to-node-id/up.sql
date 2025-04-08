CREATE TABLE IF NOT EXISTS omicron.public.cockroachdb_zone_id_to_node_id (
    omicron_zone_id UUID NOT NULL UNIQUE,
    crdb_node_id TEXT NOT NULL UNIQUE,
    PRIMARY KEY (omicron_zone_id, crdb_node_id)
);
