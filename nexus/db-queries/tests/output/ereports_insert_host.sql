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
        ($1, $2, DEFAULT, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, DEFAULT),
        ($13, $14, DEFAULT, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, DEFAULT)
      ON CONFLICT
        (restart_id, ena)
      DO
        NOTHING
      RETURNING
        ereport.ena
    ),
  inserted_reporter
    AS (
      INSERT
      INTO
        ereporter_restart (id, time_first_seen, reporter, slot_type, slot, rack_id)
      VALUES
        ($25, $26, $27, $28, $29, $30)
      ON CONFLICT
        (id)
      DO
        UPDATE SET slot = $31
      RETURNING
        id
    )
SELECT
  count(*)
FROM
  inserted_ereports
