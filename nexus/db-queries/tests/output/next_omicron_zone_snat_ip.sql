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
            r.id AS ip_pool_range_id,
            CASE $15 BETWEEN r.first_address AND r.last_address
            WHEN true THEN $16
            ELSE NULL
            END
              AS candidate_ip,
            $17 AS candidate_first_port,
            $18 AS candidate_last_port
          FROM
            ip_pool_range AS r
          WHERE
            ip_pool_id = $19 AND time_deleted IS NULL
        )
      WHERE
        candidate_ip IS NOT NULL
    ),
  validate_previously_allocated_ip
    AS MATERIALIZED (
      SELECT
        CAST(
          IF(
            NOT EXISTS(SELECT 1 FROM previously_allocated_ip)
            OR (
                SELECT
                  ip = old_ip AND first_port = old_first_port AND last_port = old_last_port
                FROM
                  next_external_ip
                  INNER JOIN previously_allocated_ip ON
                      previously_allocated_ip.old_id = next_external_ip.id
              ),
            'TRUE',
            'Reallocation of IP with different value'
          )
            AS BOOL
        )
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
