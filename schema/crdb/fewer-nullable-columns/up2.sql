SET LOCAL disallow_full_table_scans = 'off';
DELETE FROM omicron.public.internet_gateway_ip_address
WHERE internet_gateway_id IS NULL;

ALTER TABLE omicron.public.internet_gateway_ip_address
ALTER COLUMN internet_gateway_id SET NOT NULL;
