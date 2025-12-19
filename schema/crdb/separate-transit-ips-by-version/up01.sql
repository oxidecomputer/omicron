/* Add the new column for IPv6 transit IPs. */
ALTER TABLE
omicron.public.network_interface
ADD COLUMN IF NOT EXISTS transit_ips_v6
INET[] NOT NULL DEFAULT ARRAY[];
