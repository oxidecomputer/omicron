-- Copy existing fleet-scoped pools over to the pool-silo join table 
--
-- Fleet-scoped pools are going away, but we recreate the equivalent of a fleet
-- link for existing fleet-scoped pools by associating them with every existing
-- silo, i.e., inserting a row into the association table for each (pool, silo)
-- pair.
set local disallow_full_table_scans = off;

INSERT INTO omicron.public.ip_pool_resource (ip_pool_id, resource_type, resource_id, is_default)
SELECT 
  p.id AS ip_pool_id,
  'silo' AS resource_type,
  s.id AS resource_id,
  -- Special handling is required for conflicts between a fleet default and a
  -- silo default. If pool P1 is a fleet default and pool P2 is a silo default
  -- on silo S1, we cannot link both to S1 with is_default = true. What we
  -- really want in that case is:
  -- 
  --   row 1: (P1, S1, is_default=false)
  --   row 2: (P2, S1, is_default=true)
  -- 
  -- i.e., we want to link both, but have the silo default take precedence. The
  -- AND NOT EXISTS here causes is_default to be false in row 1 if there is a
  -- conflicting silo default pool. row 2 is inserted in up5.
  p.is_default AND NOT EXISTS (
    SELECT 1 
    FROM omicron.public.ip_pool p0
    WHERE p0.silo_id = s.id 
      AND p0.is_default
      AND p0.time_deleted IS NULL
  )
FROM omicron.public.ip_pool AS p
-- cross join means we are looking at the cartesian product of all fleet-scoped
-- IP pools and all silos
CROSS JOIN omicron.public.silo AS s
WHERE p.time_deleted IS null 
  AND p.silo_id IS null -- means it's a fleet pool
  AND s.time_deleted IS null
-- this makes it idempotent
ON CONFLICT (ip_pool_id, resource_type, resource_id)
DO NOTHING;
