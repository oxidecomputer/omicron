SET disallow_full_table_scans = 'off';
UPDATE omicron.public.ip_pool SET ip_version = 'v4';
SET disallow_full_table_scans = 'on';
