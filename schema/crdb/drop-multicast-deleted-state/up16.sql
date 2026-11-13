CREATE INDEX IF NOT EXISTS multicast_group_cleanup ON omicron.public.multicast_group (
    state,
    id
);
