-- RPW queries for active groups (Creating, Active states)
-- Supports: SELECT ... WHERE state = ? AND time_deleted IS NULL ORDER BY id
-- Optimizes the common case of querying non-deleted groups by state with pagination
CREATE INDEX IF NOT EXISTS multicast_group_active ON omicron.public.multicast_group (
    state,
    id
) WHERE time_deleted IS NULL;
