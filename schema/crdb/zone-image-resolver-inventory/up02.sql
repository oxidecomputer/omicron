-- Add constraints for zone image resolver columns.
ALTER TABLE omicron.public.inv_sled_agent
    ADD CONSTRAINT IF NOT EXISTS zone_manifest_consistency CHECK (
        (zone_manifest_mupdate_id IS NULL
            AND zone_manifest_boot_disk_error IS NOT NULL)
        OR
        (zone_manifest_mupdate_id IS NOT NULL
            AND zone_manifest_boot_disk_error IS NULL)
    ),
    ADD CONSTRAINT IF NOT EXISTS mupdate_override_consistency CHECK (
        (mupdate_override_id IS NULL
            AND mupdate_override_boot_disk_error IS NOT NULL)
        OR mupdate_override_boot_disk_error IS NULL
    );
