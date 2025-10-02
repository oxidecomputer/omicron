WITH
  ip_pool
    AS (
      SELECT
        CAST(IF(is_delegated, 'bad-link-type', $1) AS UUID) AS id
      FROM
        ip_pool
      WHERE
        id = $2 AND time_deleted IS NULL
    ),
  silo AS (SELECT id FROM silo WHERE id = $3 AND time_deleted IS NULL)
INSERT
INTO
  ip_pool_resource (ip_pool_id, resource_type, resource_id, is_default)
SELECT
  CAST(COALESCE(CAST(ip.id AS STRING), 'ip-pool-deleted') AS UUID),
  $4,
  CAST(COALESCE(CAST(s.id AS STRING), 'silo-deleted') AS UUID),
  $5
FROM
  (SELECT 1) AS dummy LEFT JOIN ip_pool AS ip ON true LEFT JOIN silo AS s ON true
RETURNING
  *
