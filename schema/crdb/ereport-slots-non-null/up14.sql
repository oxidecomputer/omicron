CREATE INDEX IF NOT EXISTS lookup_ereports_by_slot
ON omicron.public.ereport (
    slot_type,
    slot,
    time_collected
)
WHERE
    time_deleted IS NULL;
