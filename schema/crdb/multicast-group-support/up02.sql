-- Multicast group state for RPW
CREATE TYPE IF NOT EXISTS omicron.public.multicast_group_state AS ENUM (
    'creating',
    'active',
    'deleting',
    'deleted'
);
