-- Saga unwinding hard deletion by group
-- Supports: DELETE FROM multicast_group_member WHERE external_group_id = ?
CREATE INDEX IF NOT EXISTS multicast_member_hard_delete_by_group ON omicron.public.multicast_group_member (
    external_group_id
);
