-- Instance lifecycle state transitions optimization
-- Supports: UPDATE ... WHERE parent_id = ? AND state IN (?, ?) AND time_deleted IS NULL
CREATE INDEX IF NOT EXISTS multicast_member_parent_state ON omicron.public.multicast_group_member (
    parent_id,
    state
) WHERE time_deleted IS NULL;
