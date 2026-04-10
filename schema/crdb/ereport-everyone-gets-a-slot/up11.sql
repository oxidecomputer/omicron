CREATE INDEX IF NOT EXISTS lookup_ereports_by_location
ON omicron.public.ereport (
    slot_type,
    slot,
    time_collected
)
WHERE
    time_deleted IS NULL;
