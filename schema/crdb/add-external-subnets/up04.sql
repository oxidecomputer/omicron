CREATE INDEX IF NOT EXISTS lookup_subnet_pool_member_by_subnet_pool_id
ON omicron.public.subnet_pool_member (subnet_pool_id);
