CREATE UNIQUE INDEX IF NOT EXISTS lookup_non_deleted_oximeter ON omicron.public.oximeter (
    id
) WHERE
    time_deleted IS NULL;
