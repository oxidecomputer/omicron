CREATE TABLE IF NOT EXISTS omicron.public.bp_omicron_zone (
    blueprint_id UUID NOT NULL,
    sled_id UUID NOT NULL,
    id UUID NOT NULL,
    underlay_address INET NOT NULL,
    zone_type omicron.public.zone_type NOT NULL,
    primary_service_ip INET NOT NULL,
    primary_service_port INT4
        CHECK (primary_service_port BETWEEN 0 AND 65535)
        NOT NULL,
    second_service_ip INET,
    second_service_port INT4
        CHECK (second_service_port IS NULL
        OR second_service_port BETWEEN 0 AND 65535),
    dataset_zpool_name TEXT,
    bp_nic_id UUID,
    dns_gz_address INET,
    dns_gz_address_index INT8,
    ntp_ntp_servers TEXT[],
    ntp_dns_servers INET[],
    ntp_domain TEXT,
    nexus_external_tls BOOLEAN,
    nexus_external_dns_servers INET ARRAY,
    snat_ip INET,
    snat_first_port INT4
        CHECK (snat_first_port IS NULL OR snat_first_port BETWEEN 0 AND 65535),
    snat_last_port INT4
        CHECK (snat_last_port IS NULL OR snat_last_port BETWEEN 0 AND 65535),

    PRIMARY KEY (blueprint_id, id)
);
