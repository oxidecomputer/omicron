-- State-based filtering for RPW reconciler
-- Supports: SELECT ... WHERE state = ? AND time_deleted IS NULL
CREATE INDEX IF NOT EXISTS multicast_group_by_state ON omicron.public.multicast_group (
    state
) WHERE time_deleted IS NULL;
