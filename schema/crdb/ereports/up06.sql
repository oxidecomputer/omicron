CREATE INDEX IF NOT EXISTS lookup_host_ereports_by_sled ON omicron.public.host_ereport (
    sled_id,
    time_collected
)
WHERE
    time_deleted IS NULL;
