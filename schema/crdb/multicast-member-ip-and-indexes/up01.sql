-- Add multicast_ip column to multicast_group_member and update indexes
--
-- This migration:
-- 1. Drops redundant/unused indexes on multicast_group
-- 2. Creates optimized indexes for RPW reconciler queries
-- 3. Adds multicast_ip to multicast_group_member (denormalized for API responses)

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
