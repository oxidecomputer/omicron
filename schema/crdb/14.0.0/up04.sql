ALTER TABLE omicron.public.external_ip ADD CONSTRAINT null_pool_range_id IF NOT EXISTS CHECK (
	(ip_pool_id IS NULL AND ip_pool_range_id IS NULL) OR
    (ip_pool_id IS NOT NULL AND ip_pool_range_id IS NOT NULL)
);
