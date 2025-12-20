-- Add constraints for measurement columns.
ALTER TABLE omicron.public.inv_sled_agent
    ADD CONSTRAINT IF NOT EXISTS measurement_manifest_consistency CHECK (
        (measurement_manifest_source = 'installinator'
            AND measurement_manifest_mupdate_id IS NOT NULL
            AND measurement_manifest_boot_disk_error IS NULL)
        OR (measurement_manifest_source = 'sled-agent'
            AND measurement_manifest_mupdate_id IS NULL
            AND measurement_manifest_boot_disk_error IS NULL)
        OR (
            measurement_manifest_source IS NULL
            AND measurement_manifest_mupdate_id IS NULL
            AND measurement_manifest_boot_disk_error IS NOT NULL
        )
    );
