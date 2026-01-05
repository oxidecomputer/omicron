-- Add denormalized columns (nullable initially for backfill)
ALTER TABLE omicron.public.ip_pool_resource
    ADD COLUMN IF NOT EXISTS pool_type omicron.public.ip_pool_type,
    ADD COLUMN IF NOT EXISTS ip_version omicron.public.ip_version;
