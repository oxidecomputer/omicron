ALTER TABLE omicron.public.multicast_group
    ADD COLUMN IF NOT EXISTS state_temp omicron.public.multicast_group_state_temp;
