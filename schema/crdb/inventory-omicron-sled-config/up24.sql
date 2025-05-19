CREATE TABLE IF NOT EXISTS omicron.public.inv_omicron_sled_config_dataset (
    inv_collection_id UUID NOT NULL,
    sled_config_id UUID NOT NULL,
    id UUID NOT NULL,
    pool_id UUID NOT NULL,
    kind omicron.public.dataset_kind NOT NULL,
    zone_name TEXT,
    quota INT8,
    reservation INT8,
    compression TEXT NOT NULL,
    CONSTRAINT zone_name_for_zone_kind CHECK (
      (kind != 'zone') OR
      (kind = 'zone' AND zone_name IS NOT NULL)
    ),
    PRIMARY KEY (inv_collection_id, sled_config_id, id)
);
