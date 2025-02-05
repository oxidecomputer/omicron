CREATE TABLE IF NOT EXISTS omicron.public.tuf_repo_artifact (
    tuf_repo_id UUID NOT NULL,
    tuf_artifact_id UUID NOT NULL,
    PRIMARY KEY (tuf_repo_id, tuf_artifact_id)
)
