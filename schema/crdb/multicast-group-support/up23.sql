-- RPW reconciler sled-based switch port resolution
-- Supports: SELECT ... WHERE sled_id = ? AND time_deleted IS NULL
CREATE INDEX IF NOT EXISTS multicast_member_by_sled ON omicron.public.multicast_group_member (
    sled_id
) WHERE time_deleted IS NULL;
