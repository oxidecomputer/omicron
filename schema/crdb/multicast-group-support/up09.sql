-- IP address uniqueness and conflict detection
-- Supports: SELECT ... WHERE multicast_ip = ? AND time_deleted IS NULL
CREATE UNIQUE INDEX IF NOT EXISTS lookup_external_multicast_by_ip ON omicron.public.multicast_group (
    multicast_ip
) WHERE time_deleted IS NULL;
