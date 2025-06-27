CREATE TABLE IF NOT EXISTS omicron.public.inv_cockroachdb_status (
    inv_collection_id UUID NOT NULL,
    ranges_underreplicated INT8,
    liveness_live_nodes INT8,

    PRIMARY KEY (inv_collection_id)
);

