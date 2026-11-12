ALTER TABLE omicron.public.multicast_group_member
    ADD COLUMN IF NOT EXISTS parent_kind
        omicron.public.multicast_group_member_parent_kind
        NOT NULL DEFAULT 'instance';
