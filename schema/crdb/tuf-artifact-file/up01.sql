CREATE TABLE IF NOT EXISTS omicron.public.tuf_artifact_file (
    sha256 STRING(64) PRIMARY KEY,
    version STRING(64) NOT NULL,
    artifact_size INT8 NOT NULL
);
