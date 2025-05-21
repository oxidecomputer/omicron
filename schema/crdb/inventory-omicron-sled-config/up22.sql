CREATE TABLE IF NOT EXISTS omicron.public.inv_omicron_sled_config_zone_nic (
    inv_collection_id UUID NOT NULL,
    sled_config_id UUID NOT NULL,
    id UUID NOT NULL,
    name TEXT NOT NULL,
    ip INET NOT NULL,
    mac INT8 NOT NULL,
    subnet INET NOT NULL,
    vni INT8 NOT NULL,
    is_primary BOOLEAN NOT NULL,
    slot INT2 NOT NULL,
    PRIMARY KEY (inv_collection_id, sled_config_id, id)
);
