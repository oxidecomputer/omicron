-- Reflects that a particular artifact was provided by a particular TUF repo.
-- This is a many-many mapping.
CREATE TABLE IF NOT EXISTS omicron.public.tuf_repo_artifact (
    tuf_repo_id UUID NOT NULL,
    tuf_artifact_name STRING(63) NOT NULL,
    tuf_artifact_version STRING(63) NOT NULL,
    tuf_artifact_kind STRING(63) NOT NULL,

    /*
    For the primary key, this definition uses the natural key rather than a
    smaller surrogate key (UUID). That's because with CockroachDB the most
    important factor in selecting a primary key is the ability to distribute
    well. In this case, the first element of the primary key is the tuf_repo_id,
    which is a random UUID.

    For more, see https://www.cockroachlabs.com/blog/how-to-choose-a-primary-key/.
    */
    PRIMARY KEY (
        tuf_repo_id, tuf_artifact_name, tuf_artifact_version, tuf_artifact_kind
    )
);
