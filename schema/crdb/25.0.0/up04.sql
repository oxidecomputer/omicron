-- initialise external ip state for attached IPs.
set
  local disallow_full_table_scans = off;

UPDATE omicron.public.external_ip
SET state = 'attached'
WHERE parent_id IS NOT NULL;
