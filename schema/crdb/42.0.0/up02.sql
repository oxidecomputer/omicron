CREATE TABLE IF NOT EXISTS omicron.public.inv_zpool (
    inv_collection_id UUID NOT NULL,
    time_collected TIMESTAMPTZ NOT NULL,

    id UUID NOT NULL,
    sled_id UUID NOT NULL,
    total_size INT NOT NULL,

    PRIMARY KEY (inv_collection_id, sled_id, id)
);
