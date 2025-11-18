-- Set the temporary column to `system_internal` where the original column had
-- `oxide_internal`. This is effectively the variant-renaming itself.
SET LOCAL disallow_full_table_scans = 'off';
UPDATE omicron.public.ip_pool
SET reservation_type_temp = 'system_internal'
WHERE reservation_type = 'oxide_internal';
