set local disallow_full_table_scans = off;

-- Adds quotas for any existing silos without them. 
-- The selected quotas are based on the resources of a half rack
-- with 30% CPU and memory reserved for internal use and a 3.5x tax
-- on storage for replication, etc.
INSERT INTO
  silo_quotas (
    silo_id,
    time_created,
    time_modified,
    cpus,
    memory_bytes,
    storage_bytes
  )
SELECT
  s.id AS silo_id,
  NOW() AS time_created,
  NOW() AS time_modified,
  -- ~70% of 128 threads leaving 30% for internal use
  90 AS cpus,
  -- 708 GiB (~70% of memory leaving 30% for internal use)
  760209211392 AS memory_bytes,
  -- 850 GiB (total storage / 3.5)
  912680550400 AS storage_bytes
FROM
  silo s
  LEFT JOIN silo_quotas sq ON s.id = sq.silo_id
WHERE
  sq.silo_id IS NULL;