-- Drop mvlan column and constraint from multicast_group
-- This field was for egress multicast which is not in MVP scope

-- First drop the constraint, then the column
ALTER TABLE omicron.public.multicast_group DROP CONSTRAINT IF EXISTS mvlan_valid_range;
ALTER TABLE omicron.public.multicast_group DROP COLUMN IF EXISTS mvlan;
