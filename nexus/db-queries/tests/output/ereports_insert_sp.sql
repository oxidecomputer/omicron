WITH
  inserted_ereports
    AS (
      INSERT
      INTO
        ereport
          (
            restart_id,
            ena,
            time_deleted,
            time_collected,
            collector_id,
            part_number,
            serial_number,
            class,
            report,
            reporter,
            sled_id,
            slot_type,
            slot,
            marked_seen_in
          )
      VALUES
        ($1, $2, DEFAULT, $3, $4, $5, $6, $7, $8, $9, DEFAULT, $10, $11, DEFAULT),
        ($12, $13, DEFAULT, $14, $15, $16, $17, $18, $19, $20, DEFAULT, $21, $22, DEFAULT)
      ON CONFLICT
        (restart_id, ena)
      DO
        NOTHING
    ),
  inserted_reporter
    AS (
      INSERT
      INTO
        ereporter_restart (id, time_first_seen, reporter, slot_type, slot)
      VALUES
        ($23, $24, $25, $26, $27)
      ON CONFLICT
        (id)
      DO
        UPDATE SET slot = $28
    ),
  latest
    AS (
      SELECT
        ereport.restart_id, ereport.ena
      FROM
        ereport
      WHERE
        (ereport.slot_type = $29 AND ereport.slot = $30) AND (ereport.time_deleted IS NULL)
      ORDER BY
        ereport.time_collected DESC, ereport.ena DESC
      LIMIT
        $31
    )
SELECT
  (inserted_ereports, latest.*)
