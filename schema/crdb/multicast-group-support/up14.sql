-- Fleet-wide unique name constraint (groups are fleet-scoped like IP pools)
-- Supports: SELECT ... WHERE name = ? AND time_deleted IS NULL
CREATE UNIQUE INDEX IF NOT EXISTS lookup_multicast_group_by_name ON omicron.public.multicast_group (
    name
) WHERE time_deleted IS NULL;
