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
      RETURNING
        ereport.ena
    ),
  inserted_reporter
    AS (
      INSERT
      INTO
        ereporter_restart
          (id, time_first_seen, reporter, slot_type, slot, rack_id, time_latest_ereport_received)
      VALUES
        ($23, $24, $25, $26, $27, $28, $29)
      ON CONFLICT
        (id)
      DO
        UPDATE SET
          time_latest_ereport_received
            = greatest(
              ereporter_restart.time_latest_ereport_received,
              excluded.time_latest_ereport_received
            ),
          slot = COALESCE(ereporter_restart.slot, excluded.slot)
      RETURNING
        id
    )
SELECT
  count(*)
FROM
  inserted_ereports
