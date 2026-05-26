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
              p.created_at_generation < $3
              AND NOT
                  EXISTS(
                    SELECT
                      1
                    FROM
                      test_schema.dummy_request AS r
                    WHERE
                      r.sitrep_id = $4 AND r.id = p.dummy_id
                  )
          )
      RETURNING
        dummy_id
    )
SELECT
  (SELECT count(*) FROM deleted) AS rows_deleted,
  CASE
  WHEN (SELECT count(*) FROM page) >= $5 THEN (SELECT max(dummy_id) FROM page)
  ELSE NULL
  END
    AS next_cursor
