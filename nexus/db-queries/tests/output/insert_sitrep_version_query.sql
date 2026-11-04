WITH
  current_sitrep
    AS (
      SELECT version, sitrep_id FROM omicron.public.fm_sitrep_history ORDER BY version DESC LIMIT 1
    ),
  check_validity
    AS MATERIALIZED (
      SELECT
        CAST(
          IF(
            (
              SELECT
                parent_sitrep_id
              FROM
                omicron.public.fm_sitrep, current_sitrep
              WHERE
                id = $1 AND current_sitrep.sitrep_id = parent_sitrep_id
            ) IS NOT NULL
            OR (
                SELECT
                  1
                FROM
                  omicron.public.fm_sitrep
                WHERE
                  id = $2
                  AND parent_sitrep_id IS NULL
                  AND NOT EXISTS(SELECT version FROM current_sitrep)
              )
              = 1,
            CAST($3 AS STRING),
            'parent-not-current'
          )
            AS UUID
        )
    ),
  new_sitrep
    AS (
      SELECT
        1 AS new_version
      FROM
        omicron.public.fm_sitrep
      WHERE
        id = $4 AND parent_sitrep_id IS NULL AND NOT EXISTS(SELECT version FROM current_sitrep)
      UNION
        SELECT
          current_sitrep.version + 1
        FROM
          current_sitrep, omicron.public.fm_sitrep
        WHERE
          id = $5 AND parent_sitrep_id IS NOT NULL AND parent_sitrep_id = current_sitrep.sitrep_id
    )
INSERT
INTO
  omicron.public.fm_sitrep_history (version, sitrep_id, time_made_current)
SELECT
  new_sitrep.new_version, $6, now()
FROM
  new_sitrep
