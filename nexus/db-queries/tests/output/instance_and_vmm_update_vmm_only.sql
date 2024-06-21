WITH
  vmm_found AS (SELECT (SELECT vmm.id FROM vmm WHERE vmm.id = $1) AS id),
  vmm_updated
    AS (
      UPDATE
        vmm
      SET
        time_state_updated = $2, state_generation = $3, state = $4
      WHERE
        ((vmm.time_deleted IS NULL) AND vmm.id = $5) AND vmm.state_generation < $6
      RETURNING
        id
    ),
  vmm_result
    AS (
      SELECT
        vmm_found.id AS found, vmm_updated.id AS updated
      FROM
        vmm_found LEFT JOIN vmm_updated ON vmm_found.id = vmm_updated.id
    )
SELECT
  vmm_result.found, vmm_result.updated, NULL, NULL, NULL, NULL, NULL, NULL
FROM
  vmm_result
