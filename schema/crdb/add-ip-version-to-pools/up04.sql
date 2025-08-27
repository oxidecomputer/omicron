/*
 * All rows were given the only value that's currently valid
 * in the previous migration file. Set the column to be non-null
 * now that we've filled all the data in.
 */
ALTER TABLE omicron.public.ip_pool
ALTER COLUMN ip_version
SET NOT NULL;
