CREATE INDEX IF NOT EXISTS multicast_group_active ON omicron.public.multicast_group (
    state,
    id
) WHERE time_deleted IS NULL;
