/*
 * First, add the IP version column that is nullable.
 * The next migration file will update all the rows with the currently-valid
 * value of 'v4`, and then we'll drop the nullability at the end.
 */
ALTER TABLE IF EXISTS omicron.public.ip_pool
ADD COLUMN IF NOT EXISTS ip_version omicron.public.ip_version;
