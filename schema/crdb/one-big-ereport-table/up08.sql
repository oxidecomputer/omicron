CREATE INDEX IF NOT EXISTS un_deleted_ereports
ON omicron.public.ereport (
    time_deleted
)
STORING (
    time_collected,
    collector_id,
    serial_number,
    part_number,
    reporter,
    sled_id,
    sp_type,
    sp_slot,
    class
);
