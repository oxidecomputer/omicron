-- Pagination optimization for group member listing
-- Supports: SELECT ... WHERE external_group_id = ? ORDER BY id LIMIT ? OFFSET ?
CREATE INDEX IF NOT EXISTS multicast_member_group_id_order ON omicron.public.multicast_group_member (
    external_group_id,
    id
) WHERE time_deleted IS NULL;
