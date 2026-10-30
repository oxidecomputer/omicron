WITH
  found
    AS (
      SELECT
        active_problem.id,
        active_problem.time_created,
        active_problem.time_modified,
        active_problem.time_deleted,
        active_problem.current_revision
      FROM
        active_problem
      WHERE
        active_problem.id = $1
    ),
  updated
    AS (
      UPDATE
        active_problem
      SET
        current_revision = $2,
        time_modified
          = (
            SELECT
              active_problem_revision.time_created
            FROM
              active_problem_revision
            WHERE
              active_problem_revision.problem_id = $3 AND active_problem_revision.revision = $4
            LIMIT
              $5
          )
      WHERE
        (
          (active_problem.id = $6 AND (active_problem.time_deleted IS NULL))
          AND active_problem.current_revision < $7
        )
        AND EXISTS(
            SELECT
              active_problem_revision.problem_id,
              active_problem_revision.revision,
              active_problem_revision.time_created,
              active_problem_revision.time_resolved,
              active_problem_revision.code,
              active_problem_revision.summary,
              active_problem_revision.description,
              active_problem_revision.action_required
            FROM
              active_problem_revision
            WHERE
              active_problem_revision.problem_id = $8 AND active_problem_revision.revision = $9
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
