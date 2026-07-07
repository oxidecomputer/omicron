WITH
  overlap
    AS MATERIALIZED (
      SELECT
        CAST(IF((ipv4_block && $1), $2, $3) AS BOOL)
      FROM
        vpc_subnet
      WHERE
        vpc_id = $4 AND time_deleted IS NULL AND id != $5 AND (ipv4_block && $6 OR ipv6_block && $7)
    )
INSERT
INTO
  vpc_subnet
VALUES
  ($8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
ON CONFLICT
  (id)
DO
  UPDATE SET id = $19
RETURNING
  *
