WITH
  current_target AS (SELECT version, blueprint_id FROM bp_target ORDER BY version DESC LIMIT 1),
  check_validity
    AS MATERIALIZED (
      SELECT
        CAST(
          IF(
            (SELECT id FROM blueprint WHERE id = $1) IS NULL,
            'no-such-blueprint',
            IF(
              (
                SELECT
                  parent_blueprint_id
                FROM
                  blueprint, current_target
                WHERE
                  id = $2 AND current_target.blueprint_id = parent_blueprint_id
              ) IS NOT NULL
              OR (
                  SELECT
                    1
                  FROM
                    blueprint
                  WHERE
                    id = $3
                    AND parent_blueprint_id IS NULL
                    AND NOT EXISTS(SELECT version FROM current_target)
                )
                = 1,
              CAST($4 AS STRING),
              'parent-not-target'
            )
          )
            AS UUID
        )
    ),
  new_target
    AS (
      SELECT
        1 AS new_version
      FROM
        blueprint
      WHERE
        id = $5 AND parent_blueprint_id IS NULL AND NOT EXISTS(SELECT version FROM current_target)
      UNION
        SELECT
          current_target.version + 1
        FROM
          current_target, blueprint
        WHERE
          id = $6
          AND parent_blueprint_id IS NOT NULL
          AND parent_blueprint_id = current_target.blueprint_id
    )
INSERT
INTO
  bp_target (version, blueprint_id, enabled, time_made_target)
SELECT
  new_target.new_version, $7, $8, $9
FROM
  new_target
