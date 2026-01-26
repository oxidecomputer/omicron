WITH
  candidate_contains_existing_member_first
    AS (
      SELECT
        1
      FROM
        subnet_pool_member
      WHERE
        first_address BETWEEN $1 AND $2 AND time_deleted IS NULL
      LIMIT
        1
    ),
  candidate_contains_existing_member_last
    AS (
      SELECT
        1
      FROM
        subnet_pool_member
      WHERE
        last_address BETWEEN $3 AND $4 AND time_deleted IS NULL
      LIMIT
        1
    ),
  existing_member_contains_candidate_first
    AS (
      SELECT
        1
      FROM
        subnet_pool_member
      WHERE
        $5 BETWEEN first_address AND last_address AND time_deleted IS NULL
      LIMIT
        1
    ),
  existing_member_contains_candidate_last
    AS (
      SELECT
        1
      FROM
        subnet_pool_member
      WHERE
        $6 BETWEEN first_address AND last_address AND time_deleted IS NULL
      LIMIT
        1
    ),
  candidate_contains_existing_ip_pool_range_first
    AS (
      SELECT
        1
      FROM
        ip_pool_range
      WHERE
        first_address BETWEEN $7 AND $8 AND time_deleted IS NULL
      LIMIT
        1
    ),
  candidate_contains_existing_ip_pool_range_last
    AS (
      SELECT
        1
      FROM
        ip_pool_range
      WHERE
        last_address BETWEEN $9 AND $10 AND time_deleted IS NULL
      LIMIT
        1
    ),
  existing_ip_pool_range_contains_candidate_first
    AS (
      SELECT
        1
      FROM
        ip_pool_range
      WHERE
        $11 BETWEEN first_address AND last_address AND time_deleted IS NULL
      LIMIT
        1
    ),
  existing_ip_pool_range_contains_candidate_last
    AS (
      SELECT
        1
      FROM
        ip_pool_range
      WHERE
        $12 BETWEEN first_address AND last_address AND time_deleted IS NULL
      LIMIT
        1
    ),
  candidate_does_not_overlap
    AS (
      SELECT
        1
      WHERE
        NOT EXISTS(SELECT 1 FROM candidate_contains_existing_member_first)
        AND NOT EXISTS(SELECT 1 FROM candidate_contains_existing_member_last)
        AND NOT EXISTS(SELECT 1 FROM existing_member_contains_candidate_first)
        AND NOT EXISTS(SELECT 1 FROM existing_member_contains_candidate_last)
        AND NOT EXISTS(SELECT 1 FROM candidate_contains_existing_ip_pool_range_first)
        AND NOT EXISTS(SELECT 1 FROM candidate_contains_existing_ip_pool_range_last)
        AND NOT EXISTS(SELECT 1 FROM existing_ip_pool_range_contains_candidate_first)
        AND NOT EXISTS(SELECT 1 FROM existing_ip_pool_range_contains_candidate_last)
    ),
  new_record_values
    AS (
      SELECT
        $13 AS id,
        $14 AS time_created,
        $15 AS time_modified,
        $16 AS subnet_pool_id,
        $17 AS subnet,
        $18 AS min_prefix_length,
        $19 AS max_prefix_length,
        $20
    ),
  updated_pool
    AS (
      UPDATE
        subnet_pool
      SET
        time_modified = now(), rcgen = rcgen + 1
      WHERE
        id = $21 AND EXISTS(SELECT 1 FROM candidate_does_not_overlap) AND time_deleted IS NULL
      RETURNING
        rcgen
    )
INSERT
INTO
  subnet_pool_member
    (
      id,
      time_created,
      time_modified,
      subnet_pool_id,
      subnet,
      min_prefix_length,
      max_prefix_length,
      rcgen
    )
SELECT
  *
FROM
  new_record_values
WHERE
  EXISTS(SELECT 1 FROM candidate_does_not_overlap)
RETURNING
  id,
  time_created,
  time_modified,
  time_deleted,
  subnet_pool_id,
  subnet,
  min_prefix_length,
  max_prefix_length,
  rcgen
