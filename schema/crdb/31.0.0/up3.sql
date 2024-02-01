/* Add an index which lets us look up sleds on a rack */
CREATE UNIQUE INDEX IF NOT EXISTS lookup_bfd_session ON omicron.public.bfd_session (
    remote,
    switch
) WHERE time_deleted IS NULL;
