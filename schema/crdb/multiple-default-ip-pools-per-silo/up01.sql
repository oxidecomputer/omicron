-- Allow multiple default IP pools per silo (one per pool_type + ip_version combo)
--
-- Previously, a silo could only have one default IP pool. With multicast pool
-- support, a silo can now have up to 4 default pools: one for each combination
-- of pool_type (unicast/multicast) and ip_version (v4/v6).

-- Drop the old single-default constraint
DROP INDEX IF EXISTS omicron.public.one_default_ip_pool_per_resource;

-- Add denormalized columns (nullable initially for backfill)
ALTER TABLE omicron.public.ip_pool_resource
    ADD COLUMN IF NOT EXISTS pool_type omicron.public.ip_pool_type,
    ADD COLUMN IF NOT EXISTS ip_version omicron.public.ip_version;
