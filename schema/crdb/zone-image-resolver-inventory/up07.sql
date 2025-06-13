-- Remove default from mupdate_override_boot_disk_path.
ALTER TABLE omicron.public.inv_sled_agent ALTER COLUMN mupdate_override_boot_disk_path DROP DEFAULT;
