SELECT
  $1, $2, $3, $4, $5, $6, $7, $8
WHERE
  NOT
    EXISTS(
      SELECT
        1
      FROM
        ip_pool_range
      WHERE
        first_address <= $9 AND last_address >= $10 AND time_deleted IS NULL
      LIMIT
        1
    )
  AND NOT
      EXISTS(
        SELECT
          1
        FROM
          subnet_pool_member
        WHERE
          first_address <= $11 AND last_address >= $12 AND time_deleted IS NULL
        LIMIT
          1
      )
