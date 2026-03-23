-- Add constraint that ensures we have at least on IP, from either family.
ALTER TABLE IF EXISTS
omicron.public.network_interface
ADD CONSTRAINT IF NOT EXISTS at_least_one_ip_address CHECK (
    ip IS NOT NULL OR ipv6 IS NOT NULL
);
