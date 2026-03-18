SET LOCAL disallow_full_table_scans = 'off';

-- Delete any host OS ereports where the slot could not be resolved from
-- inventory (e.g. the sled UUID doesn't exist in any inventory collection).
DELETE FROM omicron.public.ereport
WHERE reporter = 'host'
    AND slot_type IS NULL;
