CREATE INDEX IF NOT EXISTS lookup_sp_ereports_by_slot
ON omicron.public.sp_ereport (
    sp_type,
    sp_slot,
    time_collected
)
WHERE
    time_deleted IS NULL;
