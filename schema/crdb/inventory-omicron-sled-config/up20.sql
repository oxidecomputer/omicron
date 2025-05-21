CREATE TABLE IF NOT EXISTS omicron.public.inv_omicron_sled_config_zone (
    inv_collection_id UUID NOT NULL,
    sled_config_id UUID NOT NULL,
    id UUID NOT NULL,
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
    nic_id UUID,
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
    filesystem_pool UUID,
    image_source omicron.public.inv_zone_image_source NOT NULL,
    image_artifact_sha256 STRING(64),
    CONSTRAINT zone_image_source_artifact_hash_present CHECK (
        (image_source = 'artifact'
            AND image_artifact_sha256 IS NOT NULL)
        OR
        (image_source != 'artifact'
            AND image_artifact_sha256 IS NULL)
    ),
    PRIMARY KEY (inv_collection_id, sled_config_id, id)
);
