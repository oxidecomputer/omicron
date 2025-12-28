-- Make columns NOT NULL and create new unique index
ALTER TABLE omicron.public.ip_pool_resource
    ALTER COLUMN pool_type SET NOT NULL,
    ALTER COLUMN ip_version SET NOT NULL;

-- One default pool per (resource, pool_type, ip_version) combination
CREATE UNIQUE INDEX IF NOT EXISTS one_default_ip_pool_per_resource_type_version
ON omicron.public.ip_pool_resource (
    resource_id,
    pool_type,
    ip_version
) WHERE is_default = true;
