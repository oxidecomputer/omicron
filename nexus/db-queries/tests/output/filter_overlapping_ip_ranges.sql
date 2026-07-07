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
        first_address >= $9 AND last_address <= $10 AND time_deleted IS NULL
      LIMIT
        1
    )
  AND NOT
      EXISTS(
        SELECT
          1
        FROM
          ip_pool_range
        WHERE
          first_address >= $11 AND last_address <= $12 AND time_deleted IS NULL
        LIMIT
          1
      )
  AND NOT
      EXISTS(
        SELECT
          1
        FROM
          ip_pool_range
        WHERE
          $13 >= first_address AND $14 <= last_address AND time_deleted IS NULL
        LIMIT
          1
      )
  AND NOT
      EXISTS(
        SELECT
          1
        FROM
          ip_pool_range
        WHERE
          $15 >= first_address AND $16 <= last_address AND time_deleted IS NULL
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
          first_address >= $17 AND last_address <= $18 AND time_deleted IS NULL
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
          first_address >= $19 AND last_address <= $20 AND time_deleted IS NULL
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
          $21 >= first_address AND $22 <= last_address AND time_deleted IS NULL
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
          $23 >= first_address AND $24 <= last_address AND time_deleted IS NULL
        LIMIT
          1
      )
