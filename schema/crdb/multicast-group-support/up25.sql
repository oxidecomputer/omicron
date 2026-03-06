-- Business logic constraint: one instance per group (also serves queries)
-- Supports: SELECT ... WHERE external_group_id = ? AND parent_id = ? AND time_deleted IS NULL
CREATE UNIQUE INDEX IF NOT EXISTS multicast_member_unique_parent_per_group ON omicron.public.multicast_group_member (
    external_group_id,
    parent_id
) WHERE time_deleted IS NULL;
