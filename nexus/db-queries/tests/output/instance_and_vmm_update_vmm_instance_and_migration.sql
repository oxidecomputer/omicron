WITH
  instance_found AS (SELECT (SELECT instance.id FROM instance WHERE instance.id = $1) AS id),
  instance_updated
    AS (
      UPDATE
        instance
      SET
        time_state_updated = $2,
        state_generation = $3,
        active_propolis_id = $4,
        target_propolis_id = $5,
        migration_id = $6,
        state = $7
      WHERE
        ((instance.time_deleted IS NULL) AND instance.id = $8) AND instance.state_generation < $9
      RETURNING
        id
    ),
  instance_result
    AS (
      SELECT
        instance_found.id AS found, instance_updated.id AS updated
      FROM
        instance_found LEFT JOIN instance_updated ON instance_found.id = instance_updated.id
    ),
  migration_found
    AS (
      SELECT
        (
          SELECT
            migration.id
          FROM
            migration
          WHERE
            migration.id = $10 AND (migration.time_deleted IS NULL)
        )
          AS id
    ),
  migration_updated
    AS (
      UPDATE
        migration
      SET
        source_state = $11, time_source_updated = $12
      WHERE
        (migration.id = $13 AND migration.source_propolis_id = $14) AND migration.source_gen < $15
      RETURNING
        id
    ),
  migration_result
    AS (
      SELECT
        migration_found.id AS found, migration_updated.id AS updated
      FROM
        migration_found LEFT JOIN migration_updated ON migration_found.id = migration_updated.id
    ),
  vmm_found AS (SELECT (SELECT vmm.id FROM vmm WHERE vmm.id = $16) AS id),
  vmm_updated
    AS (
      UPDATE
        vmm
      SET
        time_state_updated = $17, state_generation = $18, state = $19
      WHERE
        ((vmm.time_deleted IS NULL) AND vmm.id = $20) AND vmm.state_generation < $21
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
  instance_result.found,
  instance_result.updated,
  migration_result.found,
  migration_result.updated
FROM
  vmm_result, instance_result, migration_result
