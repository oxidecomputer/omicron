-- Version tracking for Omicron internal change detection
-- Supports: SELECT ... WHERE version_added >= ? ORDER BY version_added
CREATE UNIQUE INDEX IF NOT EXISTS underlay_multicast_group_version_added ON omicron.public.underlay_multicast_group (
    version_added
) STORING (
    multicast_ip,
    time_created,
    time_deleted
);
