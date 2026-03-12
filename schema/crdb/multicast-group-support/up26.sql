-- RPW reconciler state processing by group
-- Supports: SELECT ... WHERE external_group_id = ? AND state = ? AND time_deleted IS NULL
CREATE INDEX IF NOT EXISTS multicast_member_group_state ON omicron.public.multicast_group_member (
    external_group_id,
    state
) WHERE time_deleted IS NULL;
