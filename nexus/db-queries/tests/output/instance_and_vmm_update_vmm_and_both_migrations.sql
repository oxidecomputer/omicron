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
  migration_out_found
    AS (
      SELECT
        (
          SELECT
            migration.id
          FROM
            migration
          WHERE
            migration.id = $8 AND (migration.time_deleted IS NULL)
        )
          AS id
    ),
  migration_out_updated
    AS (
      UPDATE
        migration
      SET
        source_state = $9, time_source_updated = $10, source_gen = $11
      WHERE
        (migration.id = $12 AND migration.source_propolis_id = $13) AND migration.source_gen < $14
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
  vmm_found AS (SELECT (SELECT vmm.id FROM vmm WHERE vmm.id = $15) AS id),
  vmm_updated
    AS (
      UPDATE
        vmm
      SET
        time_state_updated = $16, state_generation = $17, state = $18
      WHERE
        ((vmm.time_deleted IS NULL) AND vmm.id = $19) AND vmm.state_generation < $20
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
  migration_out_result.found,
  migration_out_result.updated
FROM
  vmm_result, migration_in_result, migration_out_result
