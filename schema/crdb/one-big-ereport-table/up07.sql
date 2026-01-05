CREATE INDEX IF NOT EXISTS lookup_ereports_by_class
ON omicron.public.ereport (
    class
)
WHERE
    time_deleted IS NULL;
