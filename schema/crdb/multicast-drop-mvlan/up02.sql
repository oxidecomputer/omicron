-- Drop mvlan column from multicast_group
ALTER TABLE omicron.public.multicast_group DROP COLUMN IF EXISTS mvlan;
