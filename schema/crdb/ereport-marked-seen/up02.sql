CREATE INDEX IF NOT EXISTS lookup_unseen_ereports
ON omicron.public.ereport (
    restart_id, ena
)
WHERE
    marked_seen_in IS NULL
    AND time_deleted IS NULL;
