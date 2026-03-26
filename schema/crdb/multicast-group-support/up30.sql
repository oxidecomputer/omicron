-- Pagination optimization for instance member listing
-- Supports: SELECT ... WHERE parent_id = ? ORDER BY id LIMIT ? OFFSET ?
CREATE INDEX IF NOT EXISTS multicast_member_parent_id_order ON omicron.public.multicast_group_member (
    parent_id,
    id
) WHERE time_deleted IS NULL;
