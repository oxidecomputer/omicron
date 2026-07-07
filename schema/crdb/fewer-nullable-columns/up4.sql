SET LOCAL disallow_full_table_scans = 'off';
DELETE FROM omicron.public.internet_gateway_ip_pool WHERE internet_gateway_id IS NULL;
ALTER TABLE omicron.public.internet_gateway_ip_pool ALTER COLUMN internet_gateway_id SET NOT NULL;
