SET disallow_full_table_scans = 'off';
UPDATE omicron.public.ip_pool
SET name = 'oxide-service-pool-v4'
WHERE name = 'oxide-service-pool';
SET disallow_full_table_scans = 'on';
