-- Drop source_ips from multicast_group (now per-member)
ALTER TABLE omicron.public.multicast_group
    DROP COLUMN IF EXISTS source_ips;
