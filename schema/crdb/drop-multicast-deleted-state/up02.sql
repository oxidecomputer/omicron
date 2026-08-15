-- Dropping the state column requires dropping the indexes that key on it,
-- so both state indexes are dropped ahead of the type swap and recreated
-- over the new column at the end.
DROP INDEX IF EXISTS omicron.public.multicast_group_cleanup;
