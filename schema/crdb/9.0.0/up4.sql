-- copy existing fleet associations into association table. treat all existing
-- pools as fleet-associated because that is the current behavior
INSERT INTO ip_pool_resource (ip_pool_id, resource_type, resource_id, is_default)
SELECT id, 'fleet', '001de000-1334-4000-8000-000000000000', is_default
FROM ip_pool
WHERE time_deleted IS null;