ALTER TABLE omicron.public.network_interface
ADD COLUMN IF NOT EXISTS transit_ips INET[] NOT NULL DEFAULT ARRAY[];
