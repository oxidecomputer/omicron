SET LOCAL disallow_full_table_scans = 'off';
DELETE FROM omicron.public.internet_gateway_ip_pool WHERE ip_pool_id IS NULL;
ALTER TABLE omicron.public.internet_gateway_ip_pool ALTER COLUMN ip_pool_id SET NOT NULL;
