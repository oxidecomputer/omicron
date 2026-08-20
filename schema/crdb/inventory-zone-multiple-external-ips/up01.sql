CREATE TABLE IF NOT EXISTS omicron.public.inv_omicron_sled_config_zone_external_ip (
    inv_collection_id UUID NOT NULL,
    sled_config_id UUID NOT NULL,
    zone_id UUID NOT NULL,
    ip INET NOT NULL,
    port INT4
        CHECK (port IS NULL OR port BETWEEN 0 AND 65535),
    snat_first_port INT4
        CHECK (snat_first_port IS NULL OR snat_first_port BETWEEN 0 AND 65535),
    snat_last_port INT4
        CHECK (snat_last_port IS NULL OR snat_last_port BETWEEN 0 AND 65535),
    PRIMARY KEY (inv_collection_id, sled_config_id, zone_id, ip)
);
