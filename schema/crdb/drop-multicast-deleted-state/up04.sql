CREATE TYPE IF NOT EXISTS omicron.public.multicast_group_state_temp AS ENUM (
    'creating',
    'active',
    'deleting'
);
