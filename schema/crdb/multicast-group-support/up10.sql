-- Pool management and allocation queries
-- Supports: SELECT ... WHERE ip_pool_id = ? AND time_deleted IS NULL
CREATE INDEX IF NOT EXISTS external_multicast_by_pool ON omicron.public.multicast_group (
    ip_pool_id,
    ip_pool_range_id
) WHERE time_deleted IS NULL;
