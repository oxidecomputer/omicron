-- Add zone image resolver columns to the sled inventory table.
ALTER TABLE omicron.public.inv_sled_agent
   ALTER COLUMN  measurement_manifest_boot_disk_path DROP default,
   ALTER COLUMN  measurement_manifest_source DROP default,
   ALTER COLUMN  measurement_manifest_mupdate_id DROP default,
   ALTER COLUMN  measurement_manifest_boot_disk_error DROP default;

