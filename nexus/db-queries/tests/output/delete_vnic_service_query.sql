WITH
  interface
    AS MATERIALIZED (
      SELECT
        CAST(
          IF(
            (SELECT NOT is_primary FROM network_interface WHERE id = $1 AND time_deleted IS NULL)
            OR (
                SELECT
                  count(*)
                FROM
                  network_interface
                WHERE
                  parent_id = $2 AND kind = $3 AND time_deleted IS NULL
              )
              <= 1,
            $4,
            $5
          )
            AS UUID
        )
    ),
  found_interface AS (SELECT id FROM network_interface WHERE id = $6),
  updated
    AS (
      UPDATE
        network_interface
      SET
        time_deleted = now()
      WHERE
        id = $7 AND time_deleted IS NULL
      RETURNING
        id
    )
SELECT
  found_interface.id, updated.id
FROM
  found_interface LEFT JOIN updated ON found_interface.id = updated.id
