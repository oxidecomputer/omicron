-- Describes a single uploaded TUF repo.
--
-- Identified by both a random uuid and its SHA256 hash. The hash could be the
-- primary key, but it seems unnecessarily large and unwieldy.
CREATE TABLE IF NOT EXISTS omicron.public.tuf_repo (
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,

    sha256 STRING(64) NOT NULL,

    -- The version of the targets.json role that was used to generate the repo.
    targets_role_version INT NOT NULL,

    -- The valid_until time for the repo.
    valid_until TIMESTAMPTZ NOT NULL,

    -- The system version described in the TUF repo.
    --
    -- This is the "true" primary key, but is not treated as such in the
    -- database because we may want to change this format in the future.
    -- Re-doing primary keys is annoying.
    --
    -- Because the system version is embedded in the repo's artifacts.json,
    -- each system version is associated with exactly one checksum.
    system_version STRING(64) NOT NULL,

    -- For debugging only:
    -- Filename provided by the user.
    file_name TEXT NOT NULL,

    CONSTRAINT unique_checksum UNIQUE (sha256),
    CONSTRAINT unique_system_version UNIQUE (system_version)
);
