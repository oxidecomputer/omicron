WITH
  instance
    AS MATERIALIZED (
      SELECT
        CAST(
          CASE COALESCE(
            (
              SELECT
                CASE WHEN active_propolis_id IS NULL THEN state ELSE $1 END
              FROM
                instance
              WHERE
                id = $2 AND time_deleted IS NULL
            ),
            $3
          )
          WHEN $4 THEN $5
          WHEN $6 THEN $7
          WHEN $8 THEN $9
          WHEN $10 THEN $11
          ELSE $12
          END
            AS UUID
        )
    ),
  interface
    AS MATERIALIZED (
      SELECT
        CAST(
          IF(
            (SELECT NOT is_primary FROM network_interface WHERE id = $13 AND time_deleted IS NULL)
            OR (
                SELECT
                  count(*)
                FROM
                  network_interface
                WHERE
                  parent_id = $14 AND kind = $15 AND time_deleted IS NULL
              )
              <= 1,
            $16,
            $17
          )
            AS UUID
        )
    ),
  found_interface AS (SELECT id FROM network_interface WHERE id = $18),
  updated
    AS (
      UPDATE
        network_interface
      SET
        time_deleted = now()
      WHERE
        id = $19 AND time_deleted IS NULL
      RETURNING
        id
    )
SELECT
  found_interface.id, updated.id
FROM
  found_interface LEFT JOIN updated ON found_interface.id = updated.id
