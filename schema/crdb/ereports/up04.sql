CREATE INDEX IF NOT EXISTS lookup_sp_ereports_by_serial
ON omicron.public.sp_ereport (
    serial_number
) WHERE
    time_deleted IS NULL;
