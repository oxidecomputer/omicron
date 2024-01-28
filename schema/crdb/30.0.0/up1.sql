CREATE TYPE IF NOT EXISTS omicron.public.bfd_mode AS ENUM (
    'single_hop',
    'multi_hop'
);

CREATE TABLE IF NOT EXISTS omicron.public.bfd_session (
    id UUID PRIMARY KEY,
    local INET,
    remote INET NOT NULL,
    detection_threshold INT8 NOT NULL,
    required_rx INT8 NOT NULL,
    switch TEXT NOT NULL,
    mode  omicron.public.bfd_mode,

    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ
);

/* Add an index which lets us look up sleds on a rack */
CREATE UNIQUE INDEX IF NOT EXISTS lookup_bfd_session ON omicron.public.bfd_session (
    remote,
    switch
) WHERE time_deleted IS NULL;
