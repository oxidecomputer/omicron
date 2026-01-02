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

-- Add CHECK constraint to ip_pool_range to ensure data integrity
-- Ensure first address is not greater than last address
ALTER TABLE omicron.public.ip_pool_range
  ADD CONSTRAINT IF NOT EXISTS check_address_order
  CHECK (first_address <= last_address);
