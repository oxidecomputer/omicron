-- Version tracking for Omicron internal change detection
-- Supports: SELECT ... WHERE version_removed >= ? ORDER BY version_removed
CREATE UNIQUE INDEX IF NOT EXISTS multicast_member_version_removed ON omicron.public.multicast_group_member (
    version_removed
) STORING (
    external_group_id,
    parent_id,
    time_created,
    time_deleted
);
