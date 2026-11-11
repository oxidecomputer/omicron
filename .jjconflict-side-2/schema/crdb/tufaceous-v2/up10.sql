CREATE TABLE IF NOT EXISTS omicron.public.tuf_repo_metadata (
    tuf_repo_id UUID NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,

    PRIMARY KEY (tuf_repo_id, key)
);
