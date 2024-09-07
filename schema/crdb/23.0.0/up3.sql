CREATE UNIQUE INDEX IF NOT EXISTS one_default_ip_pool_per_resource ON omicron.public.ip_pool_resource (
    resource_id
) where
    is_default = true;

