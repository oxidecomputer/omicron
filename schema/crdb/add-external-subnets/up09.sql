CREATE INDEX IF NOT EXISTS lookup_subnet_pool_silo_link_by_silo_id
ON omicron.public.subnet_pool_silo_link (silo_id);
