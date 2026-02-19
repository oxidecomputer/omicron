-- Version tracking for Omicron internal change detection
-- Supports: SELECT ... WHERE version_added >= ? ORDER BY version_added
CREATE UNIQUE INDEX IF NOT EXISTS multicast_member_version_added ON omicron.public.multicast_group_member (
    version_added
) STORING (
    external_group_id,
    parent_id,
    time_created,
    time_deleted
);
