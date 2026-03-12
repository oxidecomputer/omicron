-- RPW reconciler composite queries (state + pool filtering)
-- Supports: SELECT ... WHERE state = ? AND ip_pool_id = ? AND time_deleted IS NULL
CREATE INDEX IF NOT EXISTS multicast_group_reconciler_query ON omicron.public.multicast_group (
    state,
    ip_pool_id
) WHERE time_deleted IS NULL;
