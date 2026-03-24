SET LOCAL disallow_full_table_scans = 'off';
UPDATE omicron.public.inv_nvme_disk_firmware SET slot_firmware_versions = ARRAY[]:::STRING(8)[] WHERE slot_firmware_versions IS NULL;
ALTER TABLE omicron.public.inv_nvme_disk_firmware ALTER COLUMN slot_firmware_versions SET NOT NULL;
