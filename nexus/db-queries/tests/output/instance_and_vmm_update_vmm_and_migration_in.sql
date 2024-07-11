WITH
  migration_in_found
    AS (
      SELECT
        (
          SELECT
            migration.id
          FROM
            migration
          WHERE
            migration.id = $1 AND (migration.time_deleted IS NULL)
        )
          AS id
    ),
  migration_in_updated
    AS (
      UPDATE
        migration
      SET
        target_state = $2, time_target_updated = $3, target_gen = $4
      WHERE
        (migration.id = $5 AND migration.target_propolis_id = $6) AND migration.target_gen < $7
      RETURNING
        id
    ),
  migration_in_result
    AS (
      SELECT
        migration_in_found.id AS found, migration_in_updated.id AS updated
      FROM
        migration_in_found
        LEFT JOIN migration_in_updated ON migration_in_found.id = migration_in_updated.id
    ),
  vmm_found AS (SELECT (SELECT vmm.id FROM vmm WHERE vmm.id = $8) AS id),
  vmm_updated
    AS (
      UPDATE
        vmm
      SET
        time_state_updated = $9, state_generation = $10, state = $11
      WHERE
        ((vmm.time_deleted IS NULL) AND vmm.id = $12) AND vmm.state_generation < $13
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
  vmm_result.found,
  vmm_result.updated,
  NULL,
  NULL,
  migration_in_result.found,
  migration_in_result.updated,
  NULL,
  NULL
FROM
  vmm_result, migration_in_result
