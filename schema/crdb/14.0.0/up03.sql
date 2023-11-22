ALTER TABLE omicron.public.external_ip ADD CONSTRAINT null_non_fip_pool_id IF NOT EXISTS CHECK (
	kind = 'floating' OR (ip_pool_id IS NOT NULL AND ip_pool_range_id IS NOT NULL)
);
