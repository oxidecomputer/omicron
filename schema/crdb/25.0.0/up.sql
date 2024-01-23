-- created solely to prevent a table scan when we delete links on silo delete
CREATE INDEX IF NOT EXISTS ip_pool_resource_id ON omicron.public.ip_pool_resource (
    resource_id
);
CREATE INDEX IF NOT EXISTS ip_pool_resource_ip_pool_id ON omicron.public.ip_pool_resource (
    ip_pool_id
);
