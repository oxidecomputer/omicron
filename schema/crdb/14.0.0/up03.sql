ALTER TABLE omicron.public.external_ip ADD CONSTRAINT IF NOT EXISTS null_non_fip_pool_id CHECK (
	kind = 'floating' OR (ip_pool_id IS NOT NULL AND ip_pool_range_id IS NOT NULL)
);
