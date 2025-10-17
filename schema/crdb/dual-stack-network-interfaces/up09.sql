ALTER TABLE IF EXISTS
omicron.public.network_interface
ADD CONSTRAINT IF NOT EXISTS at_least_one_ip_address CHECK (
    ipv4 IS NOT NULL OR ipv6 IS NOT NULL
);
