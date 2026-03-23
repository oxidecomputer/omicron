CREATE INDEX IF NOT EXISTS lookup_ereports_by_sp_slot
ON omicron.public.ereport (
    sp_type,
    sp_slot,
    time_collected
)
WHERE
    time_deleted IS NULL
    AND sp_type IS NOT NULL
    AND sp_slot IS NOT NULL;
