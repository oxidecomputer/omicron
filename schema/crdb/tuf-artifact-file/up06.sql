ALTER TABLE omicron.public.tuf_artifact
    DROP COLUMN IF EXISTS version,
    DROP COLUMN IF EXISTS artifact_size;
