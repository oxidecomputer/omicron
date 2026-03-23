UPDATE
  ip_pool
SET
  reservation_type = $1, time_modified = now()
WHERE
  id = $2
  AND time_deleted IS NULL
  AND reservation_type = $3
  AND CAST(
      IF(
        EXISTS(SELECT 1 FROM ip_pool_resource WHERE ip_pool_id = $4 LIMIT 1),
        'bad-link-type',
        'TRUE'
      )
        AS BOOL
    )
