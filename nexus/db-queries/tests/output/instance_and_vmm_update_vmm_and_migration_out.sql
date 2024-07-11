WITH
  migration_out_found
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
  migration_out_updated
    AS (
      UPDATE
        migration
      SET
        source_state = $2, time_source_updated = $3, source_gen = $4
      WHERE
        (migration.id = $5 AND migration.source_propolis_id = $6) AND migration.source_gen < $7
      RETURNING
        id
    ),
  migration_out_result
    AS (
      SELECT
        migration_out_found.id AS found, migration_out_updated.id AS updated
      FROM
        migration_out_found
        LEFT JOIN migration_out_updated ON migration_out_found.id = migration_out_updated.id
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
  NULL,
  NULL,
  migration_out_result.found,
  migration_out_result.updated
FROM
  vmm_result, migration_out_result
