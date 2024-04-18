-- Ensure there are no `sled_resource` rows with a `kind` other than 'instance'

-- This is a full table scan, but the sled_resource table does not track
-- historical, deleted resources, so is at most the size of the number of
-- currently-running instances (which should be zero during a schema update).
SET
  LOCAL disallow_full_table_scans = OFF;

WITH count_non_instance_resources AS (
    SELECT COUNT(*) AS num
    FROM omicron.public.sled_resource
    WHERE kind != 'instance'
)
SELECT CAST(
    IF(num = 0, 'true', 'sled_resource contains non-instance rows')
    AS bool
) FROM count_non_instance_resources;
