ALTER TABLE omicron.public.multicast_group
    ADD COLUMN IF NOT EXISTS state omicron.public.multicast_group_state;
