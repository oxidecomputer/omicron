CREATE TABLE IF NOT EXISTS omicron.public.inv_omicron_sled_config (
    inv_collection_id UUID NOT NULL,
    id UUID NOT NULL,
    generation INT8 NOT NULL,
    remove_mupdate_override UUID,
    PRIMARY KEY (inv_collection_id, id)
);
