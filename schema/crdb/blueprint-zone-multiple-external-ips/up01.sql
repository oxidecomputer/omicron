CREATE TABLE IF NOT EXISTS omicron.public.bp_omicron_zone_external_ip (
    blueprint_id UUID NOT NULL,
    zone_id UUID NOT NULL,
    external_ip_id UUID NOT NULL,
    ip INET NOT NULL,
    port INT4
        CHECK (port IS NULL OR port BETWEEN 0 AND 65535),
    snat_first_port INT4
        CHECK (snat_first_port IS NULL OR snat_first_port BETWEEN 0 AND 65535),
    snat_last_port INT4
        CHECK (snat_last_port IS NULL OR snat_last_port BETWEEN 0 AND 65535),
    PRIMARY KEY (blueprint_id, zone_id, external_ip_id)
);
