INSERT
INTO
  target_release (generation, time_requested, release_source, tuf_repo_id)
SELECT
  $1, $2, $3, $4
WHERE
  EXISTS(SELECT 1 FROM tuf_repo WHERE id = $5 AND time_pruned IS NULL)
RETURNING
  *
