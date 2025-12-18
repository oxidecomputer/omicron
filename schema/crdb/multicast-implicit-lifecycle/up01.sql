-- Add multicast_ip and source_ips columns to multicast_group_member,
-- move source_ips from group to member, and update indexes
--
-- This migration:
-- 1. Drops redundant/unused indexes on multicast_group
-- 2. Creates optimized indexes for RPW reconciler queries
-- 3. Adds multicast_ip to multicast_group_member (denormalized for API responses)
-- 4. Moves source_ips from group to member (per-member source filtering)

-- Drop redundant indexes
-- multicast_group_by_state: replaced by multicast_group_active (supports pagination)
DROP INDEX IF EXISTS omicron.public.multicast_group_by_state;

-- multicast_group_reconciler_query: unused (no queries filter by state + ip_pool_id)
DROP INDEX IF EXISTS omicron.public.multicast_group_reconciler_query;

-- RPW cleanup of soft-deleted groups
-- Supports: SELECT ... WHERE state = 'deleting' (includes rows with time_deleted set)
-- Without WHERE clause to allow queries on Deleting state regardless of time_deleted
CREATE INDEX IF NOT EXISTS multicast_group_cleanup ON omicron.public.multicast_group (
    state,
    id
);

-- RPW queries for active groups (Creating, Active states)
-- Supports: SELECT ... WHERE state = ? AND time_deleted IS NULL ORDER BY id
-- Optimizes the common case of querying non-deleted groups by state with pagination
CREATE INDEX IF NOT EXISTS multicast_group_active ON omicron.public.multicast_group (
    state,
    id
) WHERE time_deleted IS NULL;

-- Denormalized multicast IP from the group (for API convenience)
-- Note: Column added via migration, must be at end for schema compatibility
ALTER TABLE omicron.public.multicast_group_member
    ADD COLUMN IF NOT EXISTS multicast_ip INET NOT NULL;

-- Move source_ips from group to member for per-member source filtering
-- Source IPs are now stored per-member (per-join) rather than per-group.
-- This allows different members of the same group to subscribe to different
-- source addresses. Source filtering via IGMPv3/MLDv2 works with any
-- multicast address (ASM or SSM).
-- The group's source_ips in the API view is computed as the union of all
-- active member source_ips.
ALTER TABLE omicron.public.multicast_group_member
    ADD COLUMN IF NOT EXISTS source_ips INET[] DEFAULT ARRAY[]::INET[];

-- Drop source_ips from multicast_group (now per-member)
ALTER TABLE omicron.public.multicast_group
    DROP COLUMN IF EXISTS source_ips;

-- Salt for underlay IP collision avoidance (XORed into mapping)
ALTER TABLE omicron.public.multicast_group
    ADD COLUMN IF NOT EXISTS underlay_salt INT2;
