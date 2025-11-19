-- Update the final, re-typed column with the temporary values.
SET disallow_full_table_scans = 'off';
UPDATE omicron.public.ip_pool
SET reservation_type = 'system_internal'
WHERE reservation_type_temp = 'system_internal';
