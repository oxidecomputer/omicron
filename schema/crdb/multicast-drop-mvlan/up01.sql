-- Drop mvlan constraint from multicast_group
-- This field was for egress multicast which is not in MVP scope
ALTER TABLE omicron.public.multicast_group DROP CONSTRAINT IF EXISTS mvlan_valid_range;
