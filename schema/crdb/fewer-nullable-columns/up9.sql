SET LOCAL disallow_full_table_scans = 'off';
UPDATE omicron.public.multicast_group_member SET source_ips = ARRAY[]::INET[] WHERE source_ips IS NULL;
ALTER TABLE omicron.public.multicast_group_member ALTER COLUMN source_ips SET NOT NULL;
