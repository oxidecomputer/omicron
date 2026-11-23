WITH
  found
    AS (
      SELECT
        problem.id,
        problem.time_created,
        problem.time_modified,
        problem.time_deleted,
        problem.current_revision
      FROM
        problem
      WHERE
        problem.id = $1
    ),
  updated
    AS (
      UPDATE
        problem
      SET
        current_revision = $2,
        time_modified
          = (
            SELECT
              problem_revision.time_created
            FROM
              problem_revision
            WHERE
              problem_revision.problem_id = $3 AND problem_revision.revision = $4
            LIMIT
              $5
          )
      WHERE
        ((problem.id = $6 AND (problem.time_deleted IS NULL)) AND problem.current_revision < $7)
        AND EXISTS(
            SELECT
              problem_revision.problem_id,
              problem_revision.revision,
              problem_revision.time_created,
              problem_revision.time_resolved,
              problem_revision.code,
              problem_revision.summary,
              problem_revision.description,
              problem_revision.action_required
            FROM
              problem_revision
            WHERE
              problem_revision.problem_id = $8 AND problem_revision.revision = $9
          )
      RETURNING
        id
    )
SELECT
  found.id,
  updated.id,
  found.id,
  found.time_created,
  found.time_modified,
  found.time_deleted,
  found.current_revision
FROM
  found LEFT JOIN updated ON found.id = updated.id
