-- Version tracking for Omicron internal change detection
-- Supports: SELECT ... WHERE version_added >= ? ORDER BY version_added
CREATE UNIQUE INDEX IF NOT EXISTS multicast_group_version_added ON omicron.public.multicast_group (
    version_added
) STORING (
    name,
    multicast_ip,
    time_created,
    time_deleted
);
