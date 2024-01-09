CREATE TABLE IF NOT EXISTS omicron.public.inv_sled_omicron_zones (
    inv_collection_id UUID NOT NULL,
    time_collected TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL,

    sled_id UUID NOT NULL,

    generation INT8 NOT NULL,

    PRIMARY KEY (inv_collection_id, sled_id)
);
