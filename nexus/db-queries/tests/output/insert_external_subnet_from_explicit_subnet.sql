WITH
  subnet_pool_and_member
    AS (
      SELECT
        sp.id AS subnet_pool_id, spm.id AS subnet_pool_member_id, $1 AS subnet
      FROM
        subnet_pool_member AS spm
        INNER JOIN subnet_pool AS sp ON sp.id = spm.subnet_pool_id
        INNER JOIN subnet_pool_silo_link AS l ON sp.id = l.subnet_pool_id
      WHERE
        l.silo_id = $2
        AND l.ip_version = $3
        AND sp.time_deleted IS NULL
        AND spm.time_deleted IS NULL
        AND spm.first_address <= $4
        AND spm.last_address >= $5
        AND spm.min_prefix_length <= $6
        AND spm.max_prefix_length >= $7
      LIMIT
        1
    ),
  subnet_exists_in_linked_pool
    AS MATERIALIZED (
      SELECT
        CAST(IF(EXISTS((SELECT 1 FROM subnet_pool_and_member)), 'true', 'no-linked-pool') AS BOOL)
    ),
  candidate_contains_existing_first
    AS MATERIALIZED (
      SELECT
        1
      FROM
        external_subnet
      WHERE
        first_address BETWEEN $8 AND $9 AND time_deleted IS NULL
      LIMIT
        1
    ),
  candidate_contains_existing_last
    AS MATERIALIZED (
      SELECT
        1
      FROM
        external_subnet
      WHERE
        last_address BETWEEN $10 AND $11 AND time_deleted IS NULL
      LIMIT
        1
    ),
  existing_contains_candidate_first
    AS MATERIALIZED (
      SELECT
        1
      FROM
        external_subnet
      WHERE
        $12 BETWEEN first_address AND last_address AND time_deleted IS NULL
      LIMIT
        1
    ),
  existing_contains_candidate_last
    AS MATERIALIZED (
      SELECT
        1
      FROM
        external_subnet
      WHERE
        $13 BETWEEN first_address AND last_address AND time_deleted IS NULL
      LIMIT
        1
    ),
  subnet_does_not_overlap
    AS MATERIALIZED (
      SELECT
        CAST(
          IF(
            EXISTS(
              (
                SELECT
                  1
                WHERE
                  NOT EXISTS(SELECT 1 FROM candidate_contains_existing_first)
                  AND NOT EXISTS(SELECT 1 FROM candidate_contains_existing_last)
                  AND NOT EXISTS(SELECT 1 FROM existing_contains_candidate_first)
                  AND NOT EXISTS(SELECT 1 FROM existing_contains_candidate_last)
              )
            ),
            'true',
            'overlap-existing'
          )
            AS BOOL
        )
    ),
  project_is_not_deleted
    AS MATERIALIZED (
      SELECT
        CAST(
          IF(
            EXISTS(SELECT 1 FROM project WHERE id = $14 AND time_deleted IS NULL LIMIT 1),
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
            EXISTS(SELECT 1 FROM silo WHERE id = $15 AND time_deleted IS NULL LIMIT 1),
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
            project_id,
            subnet,
            attach_state,
            instance_id
          )
      SELECT
        $16 AS id,
        $17 AS name,
        $18 AS description,
        $19 AS time_created,
        $20 AS time_modified,
        NULL::TIMESTAMPTZ AS time_deleted,
        subnet_pool_id,
        subnet_pool_member_id,
        $21 AS project_id,
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
        project_id,
        subnet,
        attach_state,
        instance_id
    )
SELECT
  *
FROM
  new_record
