-- Copy existing silo-scoped pools over to the pool-silo join table 
INSERT INTO omicron.public.ip_pool_resource (ip_pool_id, resource_type, resource_id, is_default)
SELECT 
  id as ip_pool_id, 
  'silo' as resource_type,
  silo_id as resource_id,
  is_default
FROM omicron.public.ip_pool AS ip
WHERE silo_id IS NOT null
  AND time_deleted IS null
-- this makes it idempotent
ON CONFLICT (ip_pool_id, resource_type, resource_id)
DO NOTHING;
