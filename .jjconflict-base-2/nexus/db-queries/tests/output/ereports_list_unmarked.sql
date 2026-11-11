SELECT
  ereport.restart_id,
  ereport.ena,
  ereport.time_deleted,
  ereport.time_collected,
  ereport.collector_id,
  ereport.part_number,
  ereport.serial_number,
  ereport.class,
  ereport.report,
  ereport.reporter,
  ereport.sled_id,
  ereport.slot_type,
  ereport.slot,
  ereport.marked_seen_in
FROM
  ereport
WHERE
  ((ereport.marked_seen_in IS NULL) AND (ereport.time_deleted IS NULL)) AND ereport.class = ANY ($1)
ORDER BY
  ereport.restart_id ASC, ereport.ena ASC
LIMIT
  $2
