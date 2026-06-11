SELECT
  ereport.restart_id, ereport.reporter, ereport.slot_type, ereport.slot, min(ereport.time_collected)
FROM
  ereport
WHERE
  (ereport.time_deleted IS NULL) AND (ereport.slot IS NOT NULL)
GROUP BY
  ereport.restart_id, ereport.reporter, ereport.slot_type, ereport.slot
ORDER BY
  ereport.restart_id
