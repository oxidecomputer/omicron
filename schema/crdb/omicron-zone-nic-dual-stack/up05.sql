ALTER TABLE IF EXISTS
omicron.public.bp_omicron_zone_nic
ADD CONSTRAINT IF NOT EXISTS at_least_one_ip_address CHECK (
    ip IS NOT NULL OR ipv6 IS NOT NULL
);
