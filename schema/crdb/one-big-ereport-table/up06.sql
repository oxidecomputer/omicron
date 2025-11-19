CREATE INDEX IF NOT EXISTS lookup_ereports_by_serial
ON omicron.public.ereport (
    serial_number
)
STORING (
    time_collected,
    reporter,
    sp_type,
    sp_slot,
    sled_id
)
WHERE
     time_deleted IS NULL;
