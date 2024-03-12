CREATE UNIQUE INDEX IF NOT EXISTS lookup_probe_by_name ON omicron.public.probe (
    name
) WHERE
    time_deleted IS NULL;
