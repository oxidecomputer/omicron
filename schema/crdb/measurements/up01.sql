-- Add measurement image resolver columns to the sled inventory table.
ALTER TABLE omicron.public.inv_sled_agent
    -- The path to the boot disk file
    ADD COLUMN IF NOT EXISTS measurement_manifest_boot_disk_path TEXT NOT NULL DEFAULT 'old-collection-data-missing',
    -- The source of the measurement manifest on the boot disk: from installinator or
    -- sled-agent (synthetic). NULL means there is an error reading the measurement manifest.
    ADD COLUMN IF NOT EXISTS measurement_manifest_source inv_zone_manifest_source,
    -- The mupdate ID that created the measurement manifest if this is from installinator. If
    -- this is NULL, then either the measurement manifest is synthetic or there was an
    -- error reading the measurement manifest.
    ADD COLUMN IF NOT EXISTS measurement_manifest_mupdate_id UUID,
    -- Message describing the status of the measurement manifest on the boot disk. If
    -- this is NULL, then the measurement manifest was successfully read, and the
    -- inv_zone_manifest_measurement table has entries corresponding to the zone
    -- manifest.
    ADD COLUMN IF NOT EXISTS measurement_manifest_boot_disk_error TEXT DEFAULT 'old collection, data missing';


