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
  migration_in_found
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
  migration_in_updated
    AS (
      UPDATE
        migration
      SET
        target_state = $11, time_target_updated = $12, target_gen = $13
      WHERE
        (migration.id = $14 AND migration.target_propolis_id = $15) AND migration.target_gen < $16
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
  vmm_found AS (SELECT (SELECT vmm.id FROM vmm WHERE vmm.id = $17) AS id),
  vmm_updated
    AS (
      UPDATE
        vmm
      SET
        time_state_updated = $18, state_generation = $19, state = $20
      WHERE
        ((vmm.time_deleted IS NULL) AND vmm.id = $21) AND vmm.state_generation < $22
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
  migration_in_result.found,
  migration_in_result.updated,
  NULL,
  NULL
FROM
  vmm_result, instance_result, migration_in_result
