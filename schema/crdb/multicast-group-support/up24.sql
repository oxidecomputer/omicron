-- Instance-focused composite queries with group filtering
-- Supports: SELECT ... WHERE parent_id = ? AND external_group_id = ? AND time_deleted IS NULL
CREATE INDEX IF NOT EXISTS multicast_member_by_parent_and_group ON omicron.public.multicast_group_member (
    parent_id,
    external_group_id
) WHERE time_deleted IS NULL;
