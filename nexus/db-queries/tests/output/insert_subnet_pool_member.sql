WITH
  candidate_contains_existing_first
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
  candidate_contains_existing_last
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
  existing_contains_candidate_first
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
  existing_contains_candidate_last
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
  candidate_does_not_overlap
    AS (
      SELECT
        1
      WHERE
        NOT EXISTS(SELECT 1 FROM candidate_contains_existing_first)
        AND NOT EXISTS(SELECT 1 FROM candidate_contains_existing_last)
        AND NOT EXISTS(SELECT 1 FROM existing_contains_candidate_first)
        AND NOT EXISTS(SELECT 1 FROM existing_contains_candidate_last)
    ),
  new_record_values
    AS (
      SELECT
        $7 AS id,
        $8 AS time_created,
        $9 AS time_modified,
        $10 AS subnet_pool_id,
        $11 AS subnet,
        $12 AS min_prefix_length,
        $13 AS max_prefix_length,
        $14
    ),
  updated_pool
    AS (
      UPDATE
        subnet_pool
      SET
        time_modified = now(), rcgen = rcgen + 1
      WHERE
        id = $15 AND EXISTS(SELECT 1 FROM candidate_does_not_overlap) AND time_deleted IS NULL
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
