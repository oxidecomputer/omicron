-- Group membership listing and pagination
-- Supports: SELECT ... WHERE external_group_id = ? AND time_deleted IS NULL
CREATE INDEX IF NOT EXISTS multicast_member_by_external_group ON omicron.public.multicast_group_member (
    external_group_id
) WHERE time_deleted IS NULL;
