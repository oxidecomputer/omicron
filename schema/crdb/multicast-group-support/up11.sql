-- Underlay NAT group association
-- Supports: SELECT ... WHERE underlay_group_id = ? AND time_deleted IS NULL
CREATE INDEX IF NOT EXISTS external_multicast_by_underlay ON omicron.public.multicast_group (
    underlay_group_id
) WHERE time_deleted IS NULL AND underlay_group_id IS NOT NULL;
