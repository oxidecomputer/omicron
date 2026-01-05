-- Drop redundant index (replaced by multicast_group_active which supports pagination)
DROP INDEX IF EXISTS omicron.public.multicast_group_by_state;
