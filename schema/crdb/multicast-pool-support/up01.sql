-- IP Pool multicast support: Add pool types for unicast vs multicast pools

-- Add IP pool type for unicast vs multicast pools
CREATE TYPE IF NOT EXISTS omicron.public.ip_pool_type AS ENUM (
    'unicast',
    'multicast'
);
