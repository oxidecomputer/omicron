-- copy existing fleet associations into association table. treat all existing
-- pools as fleet-associated because that is the current behavior
INSERT INTO omicron.public.ip_pool_resource (ip_pool_id, resource_type, resource_id, is_default)
SELECT 
  p.id AS ip_pool_id,
  'silo' AS resource_type,
  s.id AS resource_id,
  -- note problem solved by up5.sql after this regarding is_default: if pool P1
  -- is a fleet default and pool P2 is a silo default on silo S1, we cannot link
  -- both to S1 with is_default = true. what we really want in that case is link
  -- it to S1 with is_default = false. So first, here we copy the "original"
  -- value of is_default, and then in up5 we flip is_default to false if there
  -- is a conflicting default silo-linked pool
  p.is_default
FROM ip_pool AS p
CROSS JOIN silo AS s
WHERE p.time_deleted IS null 
  AND p.silo_id IS null -- means it's a fleet pool
  AND s.time_deleted IS null
-- make this idempotent
ON CONFLICT (ip_pool_id, resource_type, resource_id)
DO NOTHING;
