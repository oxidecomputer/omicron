-- Add pool type column to ip_pool table
-- Defaults to 'unicast' for existing pools
ALTER TABLE omicron.public.ip_pool
    ADD COLUMN IF NOT EXISTS pool_type omicron.public.ip_pool_type NOT NULL DEFAULT 'unicast';
