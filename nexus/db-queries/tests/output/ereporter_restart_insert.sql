INSERT
INTO
  omicron.public.ereporter_restart (id, generation, reporter_type, slot_type, slot, time_first_seen)
SELECT
  $1,
  COALESCE(
    (
      SELECT
        max(generation)
      FROM
        omicron.public.ereporter_restart
      WHERE
        reporter_type = $2 AND slot_type = $3 AND slot = $4
      LIMIT
        1
    )
    + 1,
    0
  ),
  $5,
  $6,
  $7,
  now()
WHERE
  NOT
    EXISTS(
      SELECT
        1
      FROM
        ereporter_restart
      WHERE
        id = $8 AND reporter_type = $9 AND slot_type = $10 AND slot = $11
    )
