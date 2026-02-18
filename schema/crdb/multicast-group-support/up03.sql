-- Multicast group member state for RPW pattern
CREATE TYPE IF NOT EXISTS omicron.public.multicast_group_member_state AS ENUM (
    'joining',
    'joined',
    'left'
);
