CREATE INDEX IF NOT EXISTS external_ip_by_pool ON omicron.public.external_ip (
    ip_pool_id,
    ip_pool_range_id,
    ip
)
    WHERE time_deleted IS NULL;
