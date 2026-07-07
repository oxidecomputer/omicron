SET LOCAL disallow_full_table_scans = 'off';
DELETE FROM omicron.public.internet_gateway_ip_address
WHERE address IS NULL;

ALTER TABLE omicron.public.internet_gateway_ip_address
ALTER COLUMN address SET NOT NULL;
