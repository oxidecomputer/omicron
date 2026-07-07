-- This migration joins internet_gateway_ip_pool to ip_pool by ip_pool_id,
-- which has no index, requiring a full table scan on the smaller table.
SET LOCAL disallow_full_table_scans = 'off';

UPDATE omicron.public.internet_gateway_ip_pool AS igw_pool
SET
    name = 'default-' || ip_pool.ip_version::STRING,
    time_modified = NOW()
FROM omicron.public.ip_pool
WHERE
    igw_pool.ip_pool_id = ip_pool.id
    AND igw_pool.name = 'default'
    AND igw_pool.time_deleted IS NULL;
