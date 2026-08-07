CREATE INDEX IF NOT EXISTS lookup_unmarked_ereports_by_class
ON omicron.public.ereport (
    class, restart_id, ena
)
WHERE
    marked_seen_in IS NULL
    AND time_deleted IS NULL;
