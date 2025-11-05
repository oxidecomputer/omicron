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
          SELECT
            ip_pool_range_id, candidate_ip, candidate_first_port, candidate_last_port
          FROM
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
            (
              SELECT
                DISTINCT ON (iws.ip)
                iws.ip_pool_range_id,
                iws.ip AS candidate_ip,
                pb.candidate_first_port,
                pb.candidate_last_port
              FROM
                (
                  SELECT
                    DISTINCT e.ip_pool_range_id, e.ip
                  FROM
                    external_ip AS e
                  WHERE
                    e.ip_pool_id = $15 AND e.kind = $16 AND e.time_deleted IS NULL
                )
                  AS iws
                CROSS JOIN (
                    SELECT
                      block_start AS candidate_first_port,
                      block_start + $17 - 1 AS candidate_last_port
                    FROM
                      generate_series(0, $18, $19) AS block_start
                  )
                    AS pb
              WHERE
                NOT
                  EXISTS(
                    SELECT
                      1
                    FROM
                      external_ip AS e
                    WHERE
                      e.ip = iws.ip
                      AND e.first_port <= pb.candidate_last_port
                      AND e.last_port >= pb.candidate_first_port
                      AND e.time_deleted IS NULL
                  )
              ORDER BY
                iws.ip, pb.candidate_first_port
              LIMIT
                1
            )
          UNION ALL
            SELECT
              ip_pool_range_id,
              candidate_ip,
              0 AS candidate_first_port,
              $20 - 1 AS candidate_last_port
            FROM
              (
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
                          e.ip_pool_id = $21
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
                        e.ip_pool_id = $22
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
                                e2.ip_pool_id = $23
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
                  $24 AS candidate_last_port
                FROM
                  ip_pool_range AS r
                WHERE
                  r.ip_pool_id = $25 AND r.time_deleted IS NULL
              )
                AS free_ips
            WHERE
              candidate_ip IS NOT NULL
        )
          AS all_candidates
      ORDER BY
        candidate_ip, candidate_first_port
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
        time_modified = $26, rcgen = rcgen + 1
      WHERE
        id = (SELECT ip_pool_range_id FROM next_external_ip) AND time_deleted IS NULL
      RETURNING
        id
    )
SELECT
  *
FROM
  external_ip
