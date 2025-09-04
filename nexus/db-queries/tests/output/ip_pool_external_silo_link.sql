WITH
  ip_pool AS (SELECT id FROM ip_pool WHERE id = $1 AND time_deleted IS NULL),
  silo AS (SELECT id FROM silo WHERE id = $2 AND time_deleted IS NULL),
  existing_linked_silo
    AS (
      SELECT resource_id FROM ip_pool_resource WHERE ip_pool_id = $3 AND resource_type = $4 LIMIT 1
    )
INSERT
INTO
  ip_pool_resource (ip_pool_id, resource_type, resource_id, is_default)
SELECT
  CAST(COALESCE(CAST(ip.id AS STRING), 'ip-pool-deleted') AS UUID),
  $5,
  CAST(
    CASE
    WHEN s.id IS NULL THEN 'silo-deleted'
    WHEN (s.id = $6 AND els.resource_id = $7)
    OR (s.id != $8 AND els.resource_id != $9)
    OR els.resource_id IS NULL
    THEN $10
    ELSE 'bad-link-type'
    END
      AS UUID
  ),
  $11
FROM
  (SELECT 1) AS dummy
  LEFT JOIN ip_pool AS ip ON true
  LEFT JOIN silo AS s ON true
  LEFT JOIN existing_linked_silo AS els ON true
RETURNING
  *
