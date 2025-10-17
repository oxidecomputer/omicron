ALTER TABLE IF EXISTS
omicron.public.network_interface
ADD COLUMN IF NOT EXISTS ipv6 INET;
