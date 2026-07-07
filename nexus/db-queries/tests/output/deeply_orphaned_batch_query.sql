WITH
  batch
    AS (
      SELECT
        DISTINCT sitrep_id
      FROM
        omicron.public.fm_ereport_in_case
      WHERE
        sitrep_id > $1
      ORDER BY
        sitrep_id
      LIMIT
        $2
    ),
  deleted
    AS (
      DELETE FROM
        omicron.public.fm_ereport_in_case
      WHERE
        sitrep_id
        IN (
            SELECT
              b.sitrep_id
            FROM
              batch AS b LEFT JOIN omicron.public.fm_sitrep AS s ON s.id = b.sitrep_id
            WHERE
              s.id IS NULL
          )
      RETURNING
        sitrep_id
    )
SELECT
  (SELECT count(*) FROM deleted) AS rows_deleted,
  CASE
  WHEN (SELECT count(*) FROM batch) >= $3 THEN (SELECT max(sitrep_id) FROM batch)
  ELSE NULL
  END
    AS next_marker
