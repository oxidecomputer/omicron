CREATE UNIQUE INDEX IF NOT EXISTS
    lookup_ereporter_restart_generations_by_location
ON omicron.public.ereporter_restart (
    reporter_type,
    slot_type,
    slot,
    generation
);
