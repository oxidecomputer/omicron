-- Version tracking for Omicron internal change detection
-- Supports: SELECT ... WHERE version_removed >= ? ORDER BY version_removed
CREATE UNIQUE INDEX IF NOT EXISTS underlay_multicast_group_version_removed ON omicron.public.underlay_multicast_group (
    version_removed
) STORING (
    multicast_ip,
    time_created,
    time_deleted
);
