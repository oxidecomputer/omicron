CREATE TABLE IF NOT EXISTS omicron.public.tuf_artifact_tag (
    tuf_artifact_id UUID NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,

    PRIMARY KEY (tuf_artifact_id, key)
);
