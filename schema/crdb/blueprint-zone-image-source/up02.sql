-- Add a constraint to ensure that (kind, sha256) is unique.
CREATE UNIQUE INDEX IF NOT EXISTS tuf_artifact_kind_sha256
    ON omicron.public.tuf_artifact (kind, sha256);
