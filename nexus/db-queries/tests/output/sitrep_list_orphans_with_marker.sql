WITH
  current_sitrep_id
    AS (SELECT sitrep_id FROM omicron.public.fm_sitrep_history ORDER BY version DESC LIMIT 1),
  batch
    AS (
      SELECT
        s.id, s.parent_sitrep_id
      FROM
        omicron.public.fm_sitrep AS s
      WHERE
        s.id < $1
      ORDER BY
        s.id DESC
      LIMIT
        $2
    )
SELECT
  id
FROM
  omicron.public.fm_sitrep
WHERE
  id
  IN (
      SELECT
        b.id
      FROM
        batch AS b LEFT JOIN omicron.public.fm_sitrep_history AS h ON h.sitrep_id = b.id
      WHERE
        h.sitrep_id IS NULL
        AND (
            (b.parent_sitrep_id IS NULL AND (SELECT sitrep_id FROM current_sitrep_id) IS NOT NULL)
            OR b.parent_sitrep_id != (SELECT sitrep_id FROM current_sitrep_id)
          )
    )
