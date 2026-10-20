ALTER TABLE IF EXISTS
omicron.public.inv_omicron_sled_config_zone_nic
ADD CONSTRAINT IF NOT EXISTS ip_and_subnet_consistent CHECK (
    (ip IS NULL) = (subnet IS NULL) AND
    (ipv6 IS NULL) = (ipv6_subnet IS NULL)
);
