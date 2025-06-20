-- Add zone image resolver columns to the sled inventory table.
ALTER TABLE omicron.public.inv_sled_agent
    ADD COLUMN IF NOT EXISTS zone_manifest_boot_disk_path TEXT NOT NULL DEFAULT 'old-collection-data-missing',
    ADD COLUMN IF NOT EXISTS zone_manifest_source inv_zone_manifest_source,
    ADD COLUMN IF NOT EXISTS zone_manifest_mupdate_id UUID,
    ADD COLUMN IF NOT EXISTS zone_manifest_boot_disk_error TEXT DEFAULT 'old collection, data missing',
    ADD COLUMN IF NOT EXISTS mupdate_override_boot_disk_path TEXT NOT NULL DEFAULT 'old-collection-data-missing',
    ADD COLUMN IF NOT EXISTS mupdate_override_id UUID,
    ADD COLUMN IF NOT EXISTS mupdate_override_boot_disk_error TEXT DEFAULT 'old collection, data missing';
