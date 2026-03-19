SET LOCAL disallow_full_table_scans = 'off';
DELETE FROM omicron.public.switch_port WHERE rack_id IS NULL;
ALTER TABLE omicron.public.switch_port ALTER COLUMN rack_id SET NOT NULL;
