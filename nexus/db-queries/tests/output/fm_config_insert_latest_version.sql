WITH
  check_version
    AS MATERIALIZED (
      SELECT
        CAST(
          IF(
            (COALESCE((SELECT max(fm_config.version) FROM fm_config LIMIT $1), $2) = $3),
            'TRUE',
            'version-not-current'
          )
            AS BOOL
        )
    ),
  inserted_config
    AS (
      INSERT
      INTO
        fm_config
          (
            version,
            comment,
            time_modified,
            analysis_enabled,
            sitrep_limit,
            history_pruning_threshold
          )
      VALUES
        ($4, $5, $6, $7, $8, $9)
      RETURNING
        version
    )
SELECT
  count(*)
FROM
  inserted_config
