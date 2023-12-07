-- copy existing ip_pool-to-silo associations into association table
INSERT INTO omicron.public.ip_pool_resource (ip_pool_id, resource_type, resource_id, is_default)
SELECT id, 'silo', silo_id, is_default
FROM ip_pool
WHERE silo_id IS NOT null
  AND time_deleted IS null
-- make this idempotent
ON CONFLICT (ip_pool_id, resource_type, resource_id)
DO NOTHING;
