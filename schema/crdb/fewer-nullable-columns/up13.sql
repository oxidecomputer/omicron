SET LOCAL disallow_full_table_scans = 'off';
DELETE FROM omicron.public.switch_port WHERE switch_location IS NULL;
ALTER TABLE omicron.public.switch_port ALTER COLUMN switch_location SET NOT NULL;
