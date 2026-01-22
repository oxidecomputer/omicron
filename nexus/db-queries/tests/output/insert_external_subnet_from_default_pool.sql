WITH
  pool_id
    AS (SELECT subnet_pool_id AS id FROM subnet_pool_silo_link WHERE silo_id = $1 AND is_default),
  exactly_one_default_pool
    AS MATERIALIZED (
      SELECT
        CAST(
          CASE (SELECT count(1) FROM pool_id)
          WHEN 0 THEN 'no-linked-default'
          WHEN 1 THEN 'true'
          WHEN 2 THEN 'multiple-linked-defaults'
          END
            AS BOOL
        )
    ),
  existing_external_subnets
    AS (
      SELECT
        m.subnet_pool_id,
        m.id AS subnet_pool_member_id,
        m.first_address AS member_start,
        m.last_address AS member_end,
        m.min_prefix_length,
        m.max_prefix_length,
        e.first_address AS subnet_start,
        e.last_address AS subnet_end,
        lead(e.first_address) OVER (PARTITION BY m.id ORDER BY e.first_address) AS next_subnet_start
      FROM
        subnet_pool_member AS m
        LEFT JOIN external_subnet AS e ON e.subnet_pool_member_id = m.id AND e.time_deleted IS NULL
      WHERE
        m.subnet_pool_id = (SELECT id FROM pool_id)
        AND m.time_deleted IS NULL
        AND $2 BETWEEN m.min_prefix_length AND m.max_prefix_length
    ),
  gaps_between_subnets
    AS (
      SELECT
        subnet_pool_id,
        subnet_pool_member_id,
        CASE
        WHEN subnet_start IS NULL THEN member_start
        ELSE least(
          subnet_end,
          IF(
            "family"(subnet_end) = 4,
            '255.255.255.254'::INET,
            'ffff:ffff:ffff:ffff:ffff:ffff:ffff:fffe'::INET
          )
          + 1
        )
        END
          AS gap_start,
        CASE
        WHEN next_subnet_start IS NULL THEN member_end
        ELSE next_subnet_start - 1
        END
          AS gap_end
      FROM
        existing_external_subnets
    ),
  gaps_before_first_subnet
    AS (
      SELECT
        DISTINCT ON (m.id)
        m.subnet_pool_id,
        m.id AS subnet_pool_member_id,
        m.first_address AS gap_start,
        e.first_address - 1 AS gap_end
      FROM
        subnet_pool_member AS m
        JOIN external_subnet AS e ON e.subnet_pool_member_id = m.id AND e.time_deleted IS NULL
      WHERE
        m.subnet_pool_id = (SELECT id FROM pool_id)
        AND m.time_deleted IS NULL
        AND $3 BETWEEN m.min_prefix_length AND m.max_prefix_length
      ORDER BY
        m.id, e.first_address
    ),
  gaps AS (SELECT * FROM gaps_between_subnets UNION ALL SELECT * FROM gaps_before_first_subnet),
  candidate_subnets
    AS (
      SELECT
        subnet_pool_id,
        subnet_pool_member_id,
        gap_start,
        gap_end,
        CASE
        WHEN set_masklen(gap_start, $4) & netmask(set_masklen(gap_start, $5)) = gap_start
        THEN set_masklen(gap_start, $6)
        ELSE set_masklen(broadcast(set_masklen(gap_start, $7)) + 1, $8)
        END
          AS candidate_subnet
      FROM
        gaps
      WHERE
        gap_start < gap_end
    ),
  subnet_pool_and_member
    AS (
      SELECT
        subnet_pool_id, subnet_pool_member_id, candidate_subnet AS subnet
      FROM
        candidate_subnets
      WHERE
        candidate_subnet & netmask(candidate_subnet) >= gap_start
        AND set_masklen(broadcast(candidate_subnet), IF("family"(candidate_subnet) = 4, 32, 128))
          <= gap_end
      ORDER BY
        candidate_subnet
      LIMIT
        1
    ),
  project_is_not_deleted
    AS MATERIALIZED (
      SELECT
        CAST(
          IF(
            EXISTS(SELECT 1 FROM project WHERE id = $9 AND time_deleted IS NULL LIMIT 1),
            'true',
            'project-deleted'
          )
            AS BOOL
        )
    ),
  silo_is_not_deleted
    AS MATERIALIZED (
      SELECT
        CAST(
          IF(
            EXISTS(SELECT 1 FROM silo WHERE id = $10 AND time_deleted IS NULL LIMIT 1),
            'true',
            'silo-deleted'
          )
            AS BOOL
        )
    ),
  updated_pool
    AS (
      UPDATE
        subnet_pool
      SET
        time_modified = now(), rcgen = rcgen + 1
      WHERE
        id = (SELECT subnet_pool_id FROM subnet_pool_and_member) AND time_deleted IS NULL
      RETURNING
        1
    ),
  updated_pool_member
    AS (
      UPDATE
        subnet_pool_member
      SET
        time_modified = now(), rcgen = rcgen + 1
      WHERE
        id = (SELECT subnet_pool_member_id FROM subnet_pool_and_member) AND time_deleted IS NULL
      RETURNING
        1
    ),
  new_record
    AS (
      INSERT
      INTO
        external_subnet
          (
            id,
            name,
            description,
            time_created,
            time_modified,
            time_deleted,
            subnet_pool_id,
            subnet_pool_member_id,
            silo_id,
            project_id,
            subnet,
            attach_state,
            instance_id
          )
      SELECT
        $11 AS id,
        $12 AS name,
        $13 AS description,
        $14 AS time_created,
        $15 AS time_modified,
        NULL::TIMESTAMPTZ AS time_deleted,
        subnet_pool_id,
        subnet_pool_member_id,
        $16 AS silo_id,
        $17 AS project_id,
        subnet AS subnet,
        'detached' AS attach_state,
        NULL AS instance_id
      FROM
        subnet_pool_and_member
      RETURNING
        id,
        name,
        description,
        time_created,
        time_modified,
        time_deleted,
        subnet_pool_id,
        subnet_pool_member_id,
        silo_id,
        project_id,
        subnet,
        attach_state,
        instance_id
    )
SELECT
  *
FROM
  new_record
