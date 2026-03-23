UPDATE
  ip_pool
SET
  reservation_type = $1, time_modified = now()
WHERE
  id = $2
  AND time_deleted IS NULL
  AND reservation_type = $3
  AND (
      SELECT
        CAST(
          IF(
            EXISTS(
              SELECT
                1
              FROM
                external_ip
              WHERE
                ip_pool_id = $4 AND external_ip.is_service AND time_deleted IS NULL
              LIMIT
                1
            ),
            1 / 0,
            1
          )
            AS BOOL
        )
    )
  AND CAST(
      IF(
        (SELECT count(1) FROM ip_pool WHERE time_deleted IS NULL AND reservation_type = $5 LIMIT 2)
        >= 2,
        '1',
        $6
      )
        AS INT8
    )
    = 1
