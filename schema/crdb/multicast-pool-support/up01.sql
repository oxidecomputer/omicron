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

-- Add switch port uplinks for multicast pools (array of switch port UUIDs)
-- Only applies to multicast pools for static (operator) configuration
-- Always NULL for unicast pools
ALTER TABLE omicron.public.ip_pool
    ADD COLUMN IF NOT EXISTS switch_port_uplinks UUID[];

-- Add MVLAN ID for multicast pools
-- Only applies to multicast pools for static (operator) configuration
-- Always NULL for unicast pools
ALTER TABLE omicron.public.ip_pool
    ADD COLUMN IF NOT EXISTS mvlan INT4;

-- Add index on pool_type for efficient filtering
CREATE INDEX IF NOT EXISTS lookup_ip_pool_by_type ON omicron.public.ip_pool (
    pool_type
) WHERE
    time_deleted IS NULL;
