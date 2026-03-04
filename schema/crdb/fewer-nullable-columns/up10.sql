SET LOCAL disallow_full_table_scans = 'off';
UPDATE omicron.public.vmm SET cpu_platform = 'sled_default' WHERE cpu_platform IS NULL;
ALTER TABLE omicron.public.vmm ALTER COLUMN cpu_platform SET NOT NULL;
