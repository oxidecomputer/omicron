ALTER TABLE IF EXISTS
omicron.public.inv_omicron_sled_config_zone_nic
ADD COLUMN IF NOT EXISTS ipv6 INET;
