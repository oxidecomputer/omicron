-- Describes an individual artifact from an uploaded TUF repo.
--
-- In the future, this may also be used to describe artifacts that are fetched
-- from a remote TUF repo, but that requires some additional design work.
CREATE TABLE IF NOT EXISTS omicron.public.tuf_artifact (
    name STRING(63) NOT NULL,
    version STRING(63) NOT NULL,
    -- This used to be an enum but is now a string, because it can represent
    -- artifact kinds currently unknown to a particular version of Nexus as
    -- well.
    kind STRING(63) NOT NULL,

    -- The time this artifact was first recorded.
    time_created TIMESTAMPTZ NOT NULL,

    -- The SHA256 hash of the artifact, typically obtained from the TUF
    -- targets.json (and validated at extract time).
    sha256 STRING(64) NOT NULL,
    -- The length of the artifact, in bytes.
    artifact_size INT8 NOT NULL,

    PRIMARY KEY (name, version, kind)
);
