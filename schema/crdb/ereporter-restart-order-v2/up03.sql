CREATE INDEX IF NOT EXISTS lookup_ereporter_restart_by_slot
ON omicron.public.ereporter_restart (
    reporter,
    slot_type,
    slot
)
WHERE
    slot IS NOT NULL;
