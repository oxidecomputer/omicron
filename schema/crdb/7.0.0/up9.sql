/*
 * Because this is an offline update, the system comes back up with no active
 * VMMs. Ensure all active Propolis IDs are cleared. This guileless approach
 * gets planned as a full table scan, so explicitly (but temporarily) allow
 * those.
 */

set disallow_full_table_scans = off;
UPDATE omicron.public.instance SET active_propolis_id = NULL;
set disallow_full_table_scans = on;
