-- RPW cleanup of soft-deleted members
-- Supports: DELETE FROM multicast_group_member WHERE state = 'Left' AND time_deleted IS NOT NULL
CREATE INDEX IF NOT EXISTS multicast_member_cleanup ON omicron.public.multicast_group_member (
    state
) WHERE time_deleted IS NOT NULL;
