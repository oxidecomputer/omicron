SET LOCAL disallow_full_table_scans = 'off';
DELETE FROM omicron.public.switch_port WHERE port_name IS NULL;
ALTER TABLE omicron.public.switch_port ALTER COLUMN port_name SET NOT NULL;
