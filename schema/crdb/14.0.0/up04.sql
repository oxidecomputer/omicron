ALTER TABLE omicron.public.external_ip ADD CONSTRAINT IF NOT EXISTS null_pool_range_id CHECK (
	(ip_pool_id IS NULL AND ip_pool_range_id IS NULL) OR
    (ip_pool_id IS NOT NULL AND ip_pool_range_id IS NOT NULL)
);
