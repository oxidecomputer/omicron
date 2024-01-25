-- initialise external ip state for detached IPs.
set
  local disallow_full_table_scans = off;

UPDATE omicron.public.external_ip
SET state = 'detached'
WHERE parent_id IS NULL;
