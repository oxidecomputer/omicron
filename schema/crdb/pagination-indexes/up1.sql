CREATE UNIQUE INDEX IF NOT EXISTS lookup_pool_range_by_pool_and_first_address
    ON omicron.public.ip_pool_range (ip_pool_id, first_address)
    STORING (last_address)
    WHERE time_deleted IS NULL;
