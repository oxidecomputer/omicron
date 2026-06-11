SELECT
  ereport.restart_id,
  ereport.reporter,
  ereport.slot_type,
  max(ereport.slot),
  count(DISTINCT ereport.slot),
  min(ereport.time_collected)
FROM
  ereport
WHERE
  ereport.time_deleted IS NULL
GROUP BY
  ereport.restart_id, ereport.reporter, ereport.slot_type
HAVING
  max(ereport.slot) IS NOT NULL
