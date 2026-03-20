WITH
  previous
    AS (
      SELECT
        generation
      FROM
        omicron.public.ereporter_restart
      WHERE
        reporter_type = $1 AND slot_type = $2 AND slot = $3
      ORDER BY
        generation DESC
      LIMIT
        1
    )
INSERT
INTO
  omicron.public.ereporter_restart (id, generation, reporter_type, slot_type, slot, time_first_seen)
VALUES
  ($4, COALESCE((SELECT generation FROM previous) + 1, 0), $5, $6, $7, now())
ON CONFLICT
  (generation, reporter_type, slot_type, slot)
DO
  NOTHING
