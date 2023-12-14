-- Fleet-scoped pools are going away, but we recreate the equivalent of a fleet
-- link for existing fleet-scoped pools by associating them with every existing
-- silo, i.e., inserting a row into the association table for each (pool, silo)
-- pair.
--
-- Note special handling is required for conflicts between a fleet default and
-- a silo default. If pool P1 is a fleet default and pool P2 is a silo default
-- on silo S1, we cannot link both to S1 with is_default = true. What we really
-- want in that case is link it to S1 with is_default = false. So first, here we
-- copy the "original" value of is_default to the link between P1 and S1. Then,
-- in up5, we flip is_default to false on P1 when we see that P2 wants to be the
-- default for S1.
INSERT INTO omicron.public.ip_pool_resource (ip_pool_id, resource_type, resource_id, is_default)
SELECT 
  p.id AS ip_pool_id,
  'silo' AS resource_type,
  s.id AS resource_id,
  p.is_default
FROM omicron.public.ip_pool AS p
CROSS JOIN omicron.public.silo AS s
WHERE p.time_deleted IS null 
  AND p.silo_id IS null -- means it's a fleet pool
  AND s.time_deleted IS null
-- this makes it idempotent
ON CONFLICT (ip_pool_id, resource_type, resource_id)
DO NOTHING;
