WITH
  current_sitrep_id
    AS (SELECT sitrep_id FROM omicron.public.fm_sitrep_history ORDER BY version DESC LIMIT 1),
  batch
    AS (
      SELECT
        s.id
      FROM
        omicron.public.fm_sitrep AS s
        LEFT JOIN omicron.public.fm_sitrep_history AS h ON h.sitrep_id = s.id
      WHERE
        h.sitrep_id IS NULL
        AND s.id >= $1
        AND (
            (s.parent_sitrep_id IS NULL AND (SELECT sitrep_id FROM current_sitrep_id) IS NOT NULL)
            OR s.parent_sitrep_id != (SELECT sitrep_id FROM current_sitrep_id)
          )
      ORDER BY
        s.id
      LIMIT
        $2
    ),
  deleted AS (DELETE FROM omicron.public.fm_sitrep WHERE id IN (SELECT id FROM batch) RETURNING id)
SELECT
  (SELECT count(*) FROM deleted) AS rows_deleted,
  CASE
  WHEN (SELECT count(*) FROM batch) >= $3 THEN (SELECT max(id) FROM batch)
  ELSE NULL
  END
    AS next_marker
