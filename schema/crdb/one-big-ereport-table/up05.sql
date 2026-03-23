CREATE INDEX IF NOT EXISTS lookup_ereports_by_sled
ON omicron.public.ereport (
    sled_id,
    time_collected
)
WHERE
    sled_id IS NOT NULL
    AND time_deleted IS NULL;
