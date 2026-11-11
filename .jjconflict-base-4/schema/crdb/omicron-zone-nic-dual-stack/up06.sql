ALTER TABLE IF EXISTS
omicron.public.bp_omicron_zone_nic
ADD CONSTRAINT IF NOT EXISTS ip_and_subnet_consistent CHECK (
    (ip IS NULL) = (subnet IS NULL) AND
    (ipv6 IS NULL) = (ipv6_subnet IS NULL)
);
