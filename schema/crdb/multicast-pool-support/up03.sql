-- Add index on pool_type for efficient filtering
CREATE INDEX IF NOT EXISTS lookup_ip_pool_by_type ON omicron.public.ip_pool (
    pool_type
) WHERE
    time_deleted IS NULL;
