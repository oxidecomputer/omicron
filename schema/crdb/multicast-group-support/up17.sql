-- Admin-scoped IPv6 address uniqueness
-- Supports: SELECT ... WHERE multicast_ip = ? AND time_deleted IS NULL
CREATE UNIQUE INDEX IF NOT EXISTS lookup_underlay_multicast_by_ip ON omicron.public.underlay_multicast_group (
    multicast_ip
) WHERE time_deleted IS NULL;
