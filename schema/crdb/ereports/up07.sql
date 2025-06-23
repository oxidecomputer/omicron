CREATE INDEX IF NOT EXISTS order_host_ereports_by_timestamp
ON omicron.public.host_ereport
USING HASH (
    time_collected
)
WHERE
    time_deleted IS NULL;
