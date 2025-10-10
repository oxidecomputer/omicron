-- IP Pool multicast support: Add pool types for unicast vs multicast pools

-- Add IP pool type for unicast vs multicast pools
CREATE TYPE IF NOT EXISTS omicron.public.ip_pool_type AS ENUM (
    'unicast',
    'multicast'
);

-- Add pool type column to ip_pool table
-- Defaults to 'unicast' for existing pools
ALTER TABLE omicron.public.ip_pool
    ADD COLUMN IF NOT EXISTS pool_type omicron.public.ip_pool_type NOT NULL DEFAULT 'unicast';

-- Add index on pool_type for efficient filtering
CREATE INDEX IF NOT EXISTS lookup_ip_pool_by_type ON omicron.public.ip_pool (
    pool_type
) WHERE
    time_deleted IS NULL;
