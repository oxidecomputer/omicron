-- Add columns and constraints to bp_omicron_zone.
ALTER TABLE omicron.public.bp_omicron_zone
    ADD COLUMN IF NOT EXISTS image_source omicron.public.bp_zone_image_source NOT NULL DEFAULT 'install_dataset',
    ADD COLUMN IF NOT EXISTS image_artifact_sha256 STRING(64),
    ADD CONSTRAINT IF NOT EXISTS zone_image_source_artifact_hash_present CHECK (
        (image_source = 'artifact'
            AND image_artifact_sha256 IS NOT NULL)
        OR
        (image_source != 'artifact'
            AND image_artifact_sha256 IS NULL)
    );
