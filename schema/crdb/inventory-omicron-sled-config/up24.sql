CREATE TABLE IF NOT EXISTS omicron.public.inv_omicron_sled_config_disk (
    inv_collection_id UUID NOT NULL,
    sled_config_id UUID NOT NULL,
    id UUID NOT NULL,
    vendor TEXT NOT NULL,
    serial TEXT NOT NULL,
    model TEXT NOT NULL,
    pool_id UUID NOT NULL,
    PRIMARY KEY (inv_collection_id, sled_config_id, id)
);
