-- Drop the non-NULL constraint on the `ip` column, since an interface
-- can be IPv6-only in the future.
ALTER TABLE IF EXISTS
omicron.public.network_interface
ALTER COLUMN ip DROP NOT NULL;
