CREATE UNIQUE INDEX IF NOT EXISTS lookup_deleted_disk ON omicron.public.disk (
    id
) WHERE
    time_deleted IS NOT NULL;
