-- Denormalized multicast IP from the group (for API convenience)
ALTER TABLE omicron.public.multicast_group_member
    ADD COLUMN IF NOT EXISTS multicast_ip INET NOT NULL;
