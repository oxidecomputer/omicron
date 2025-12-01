-- Backfill pool_type and ip_version from ip_pool table
SET LOCAL disallow_full_table_scans = off;
UPDATE omicron.public.ip_pool_resource AS resource
SET
    pool_type = pool.pool_type,
    ip_version = pool.ip_version
FROM omicron.public.ip_pool AS pool
WHERE resource.ip_pool_id = pool.id
  AND (resource.pool_type IS NULL OR resource.ip_version IS NULL);
