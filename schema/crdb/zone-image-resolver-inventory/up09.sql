-- Remove default from mupdate_override_boot_disk_error.
ALTER TABLE omicron.public.inv_sled_agent ALTER COLUMN mupdate_override_boot_disk_error DROP DEFAULT;
