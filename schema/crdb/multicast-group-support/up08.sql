-- Version tracking for Omicron internal change detection
-- Supports: SELECT ... WHERE version_removed >= ? ORDER BY version_removed
CREATE UNIQUE INDEX IF NOT EXISTS multicast_group_version_removed ON omicron.public.multicast_group (
    version_removed
) STORING (
    name,
    multicast_ip,
    time_created,
    time_deleted
);
