CREATE INDEX IF NOT EXISTS lookup_ereports_by_serial
ON omicron.public.ereport (
    serial_number
)
STORING (
    time_collected,
    reporter,
    sled_id,
    slot_type,
    slot
)
WHERE
    time_deleted IS NULL;
