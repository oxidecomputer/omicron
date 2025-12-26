WITH
  ip_pool
    AS (
      SELECT
        CAST(IF(reservation_type != $1, 'bad-link-type', $2) AS UUID) AS id, pool_type, ip_version
      FROM
        ip_pool
      WHERE
        id = $3 AND time_deleted IS NULL
    ),
  silo AS (SELECT id FROM silo WHERE id = $4 AND time_deleted IS NULL)
INSERT
INTO
  ip_pool_resource (ip_pool_id, resource_type, resource_id, is_default, pool_type, ip_version)
SELECT
  CAST(COALESCE(CAST(ip.id AS STRING), 'ip-pool-deleted') AS UUID),
  $5,
  CAST(COALESCE(CAST(s.id AS STRING), 'silo-deleted') AS UUID),
  $6,
  ip.pool_type,
  ip.ip_version
FROM
  (SELECT 1) AS dummy LEFT JOIN ip_pool AS ip ON true LEFT JOIN silo AS s ON true
RETURNING
  *
