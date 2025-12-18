WITH
  previously_allocated_ip
    AS (
      SELECT
        id AS old_id,
        ip_pool_range_id AS old_ip_pool_range_id,
        ip AS old_ip,
        first_port AS old_first_port,
        last_port AS old_last_port
      FROM
        external_ip
      WHERE
        id = $1 AND time_deleted IS NULL
    ),
  next_external_ip
    AS (
      SELECT
        $2 AS id,
        $3 AS name,
        $4 AS description,
        $5 AS time_created,
        $6 AS time_modified,
        $7 AS time_deleted,
        $8 AS ip_pool_id,
        ip_pool_range_id,
        $9 AS is_service,
        $10 AS parent_id,
        $11 AS kind,
        candidate_ip AS ip,
        candidate_first_port AS first_port,
        candidate_last_port AS last_port,
        $12 AS project_id,
        $13 AS state,
        $14 AS is_probe
      FROM
        (
          (
            SELECT
              old_ip_pool_range_id AS ip_pool_range_id,
              old_ip AS candidate_ip,
              old_first_port AS candidate_first_port,
              old_last_port AS candidate_last_port
            FROM
              previously_allocated_ip
          )
          UNION ALL
            SELECT
              r.id AS ip_pool_range_id,
              COALESCE(
                CASE
                WHEN NOT
                  EXISTS(
                    SELECT
                      1
                    FROM
                      external_ip AS e
                    WHERE
                      e.ip_pool_id = $15
                      AND e.ip_pool_range_id = r.id
                      AND e.ip = r.first_address
                      AND e.time_deleted IS NULL
                  )
                THEN r.first_address
                END,
                (
                  SELECT
                    e.ip + 1
                  FROM
                    external_ip AS e
                  WHERE
                    e.ip_pool_id = $16
                    AND e.ip_pool_range_id = r.id
                    AND e.ip < r.last_address
                    AND e.time_deleted IS NULL
                    AND NOT
                        EXISTS(
                          SELECT
                            1
                          FROM
                            external_ip AS e2
                          WHERE
                            e2.ip_pool_id = $17
                            AND e2.ip_pool_range_id = r.id
                            AND e2.ip = e.ip + 1
                            AND e2.time_deleted IS NULL
                        )
                  ORDER BY
                    e.ip
                  LIMIT
                    1
                )
              )
                AS candidate_ip,
              0 AS candidate_first_port,
              $18 AS candidate_last_port
            FROM
              ip_pool_range AS r
            WHERE
              r.ip_pool_id = $19 AND r.time_deleted IS NULL
        )
          AS all_candidates
      WHERE
        candidate_ip IS NOT NULL
      ORDER BY
        candidate_ip
      LIMIT
        1
    ),
  external_ip
    AS (
      INSERT
      INTO
        external_ip
      (SELECT * FROM next_external_ip)
      ON CONFLICT
        (id)
      DO
        UPDATE SET
          time_created = excluded.time_created,
          time_modified = excluded.time_modified,
          time_deleted = excluded.time_deleted
      RETURNING
        *
    ),
  updated_pool_range
    AS (
      UPDATE
        ip_pool_range
      SET
        time_modified = $20, rcgen = rcgen + 1
      WHERE
        id = (SELECT ip_pool_range_id FROM next_external_ip) AND time_deleted IS NULL
      RETURNING
        id
    )
SELECT
  *
FROM
  external_ip
