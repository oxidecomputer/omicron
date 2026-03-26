-- Instance membership queries (all groups for an instance)
-- Supports: SELECT ... WHERE parent_id = ? AND time_deleted IS NULL
CREATE INDEX IF NOT EXISTS multicast_member_by_parent ON omicron.public.multicast_group_member (
    parent_id
) WHERE time_deleted IS NULL;
