CREATE INDEX IF NOT EXISTS inv_omicron_sled_config_zone_nic_id
    ON omicron.public.inv_omicron_sled_config_zone (nic_id)
    STORING (
        primary_service_ip,
        second_service_ip,
        snat_ip
    );
