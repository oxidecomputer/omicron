CREATE TABLE IF NOT EXISTS omicron.public.tuf_artifact (
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    version STRING(63) NOT NULL,
    kind STRING(63) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    sha256 STRING(64) NOT NULL,
    artifact_size INT8 NOT NULL,
    CONSTRAINT unique_name_version_kind UNIQUE (name, version, kind)
)
