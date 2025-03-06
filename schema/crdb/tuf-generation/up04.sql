CREATE UNIQUE INDEX IF NOT EXISTS tuf_artifact_added
    ON omicron.public.tuf_artifact (generation_added, id)
    STORING (name, version, kind, time_created, sha256, artifact_size);
