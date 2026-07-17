WITH
  page
    AS (
      SELECT
        dummy_id, created_at_generation
      FROM
        test_schema.dummy_marker
      WHERE
        dummy_id > $1
      ORDER BY
        dummy_id
      LIMIT
        $2
    ),
  deleted
    AS (
      DELETE FROM
        test_schema.dummy_marker
      WHERE
        dummy_id
        IN (
            SELECT
              p.dummy_id
            FROM
              page AS p
            WHERE
              EXISTS(SELECT 1 FROM fm_sitrep WHERE id = $3)
              AND NOT
                  EXISTS(
                    SELECT
                      1
                    FROM
                      test_schema.dummy_request AS r
                    WHERE
                      r.sitrep_id = $4 AND r.id = p.dummy_id
                  )
              AND p.created_at_generation < $5
          )
      RETURNING
        dummy_id
    )
SELECT
  (SELECT count(*) FROM deleted) AS rows_deleted,
  CASE
  WHEN (SELECT count(*) FROM page) >= $6 THEN (SELECT max(dummy_id) FROM page)
  ELSE NULL
  END
    AS next_cursor
