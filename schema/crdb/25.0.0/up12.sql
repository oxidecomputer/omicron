-- Reflects that a particular artifact was provided by a particular TUF repo.
-- This is a many-many mapping.
CREATE TABLE IF NOT EXISTS omicron.public.tuf_repo_artifact (
    tuf_repo_id UUID NOT NULL,
    tuf_artifact_name STRING(63) NOT NULL,
    tuf_artifact_version STRING(63) NOT NULL,
    tuf_artifact_kind STRING(63) NOT NULL,

    PRIMARY KEY (
        tuf_repo_id, tuf_artifact_name, tuf_artifact_version, tuf_artifact_kind
    )
);
