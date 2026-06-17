CREATE TYPE IF NOT EXISTS omicron.public.multicast_group_member_parent_kind AS ENUM (
    /* A guest instance. */
    'instance',
    /* A probe (network connectivity diagnostic). */
    'probe'
);
