-- Lifecycle management via group tags
-- Supports: SELECT ... WHERE tag = ? AND time_deleted IS NULL
CREATE INDEX IF NOT EXISTS underlay_multicast_by_tag ON omicron.public.underlay_multicast_group (
    tag
) WHERE time_deleted IS NULL AND tag IS NOT NULL;
