/*
 * Now that the sled_instance view is up-to-date, begin to drop columns from the
 * instance table that are no longer needed. This needs to be done after
 * altering the sled_instance view because it's illegal to drop columns that a
 * view depends on.
 */

ALTER TABLE omicron.public.instance DROP COLUMN IF EXISTS active_sled_id;
