ALTER TABLE IF EXISTS
omicron.public.bp_omicron_zone_nic
ADD COLUMN IF NOT EXISTS ipv6_subnet INET;
