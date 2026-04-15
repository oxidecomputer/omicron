SET LOCAL disallow_full_table_scans = 'off';
UPDATE omicron.public.inv_nvme_disk_firmware SET slot1_is_read_only = false WHERE slot1_is_read_only IS NULL;
ALTER TABLE omicron.public.inv_nvme_disk_firmware ALTER COLUMN slot1_is_read_only SET NOT NULL;
