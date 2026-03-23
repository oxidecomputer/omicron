-- Add the `ipv6` column first.
ALTER TABLE IF EXISTS
omicron.public.network_interface
ADD COLUMN IF NOT EXISTS ipv6 INET;
