-- Move source_ips from group to member for per-member source filtering.
-- Source IPs are now stored per-member (per-join) rather than per-group.
-- This allows different members of the same group to subscribe to different
-- source addresses. Source filtering via IGMPv3/MLDv2 works with any
-- multicast address (ASM or SSM).
-- The group's source_ips in the API view is computed as the union of all
-- active member source_ips.
ALTER TABLE omicron.public.multicast_group_member
    ADD COLUMN IF NOT EXISTS source_ips INET[] DEFAULT ARRAY[]::INET[];
