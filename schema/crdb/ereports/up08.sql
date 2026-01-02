CREATE INDEX IF NOT EXISTS lookup_host_ereports_by_serial
ON omicron.public.host_ereport (
    sled_serial
) WHERE
    time_deleted IS NULL;
