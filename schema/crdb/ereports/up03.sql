CREATE INDEX IF NOT EXISTS order_sp_ereports_by_timestamp
ON omicron.public.sp_ereport (
    time_collected
)
WHERE
    time_deleted IS NULL;
