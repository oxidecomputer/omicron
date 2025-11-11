CREATE INDEX IF NOT EXISTS un_deleted_ereports
ON omicron.public.ereport (
    time_deleted
)
STORING (
    time_collected,
    class,
    serial_number,
    part_number,
    sled_id,
    sp_slot,
    sp_type
);
