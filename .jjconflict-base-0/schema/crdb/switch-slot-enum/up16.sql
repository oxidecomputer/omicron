CREATE UNIQUE INDEX IF NOT EXISTS lookup_bfd_session ON omicron.public.bfd_session (
    remote,
    switch_slot
) WHERE time_deleted IS NULL;
