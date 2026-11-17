-- Dropping the state column requires dropping the indexes that are built on it.
-- We drop the associated indexes before the type swap, and then recreate
-- them over the new column at the end.
DROP INDEX IF EXISTS omicron.public.multicast_group_cleanup;
