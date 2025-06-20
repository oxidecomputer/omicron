-- Remove default from zone_manifest_boot_disk_error.
ALTER TABLE omicron.public.inv_sled_agent ALTER COLUMN zone_manifest_boot_disk_error DROP DEFAULT;
