INSERT
INTO
  ereporter_restart (id, generation, reporter_type, slot_type, slot, time_first_seen)
SELECT
  $1,
  COALESCE(
    (
      SELECT
        max(ereporter_restart.generation)
      FROM
        ereporter_restart
      WHERE
        (ereporter_restart.reporter_type = $2 AND ereporter_restart.slot_type = $3)
        AND ereporter_restart.slot = $4
      LIMIT
        $5
    )
    + $6,
    $7
  ),
  $8,
  $9,
  $10,
  current_timestamp()
WHERE
  NOT
    (
      EXISTS(
        SELECT
          $11
        FROM
          ereporter_restart
        WHERE
          (
            (ereporter_restart.id = $12 AND ereporter_restart.reporter_type = $13)
            AND ereporter_restart.slot_type = $14
          )
          AND ereporter_restart.slot = $15
      )
    )
