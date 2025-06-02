SELECT
  sp_ereport.restart_id, sp_ereport.ena
FROM
  sp_ereport
WHERE
  sp_ereport.sp_type = $1 AND sp_ereport.sp_slot = $2
ORDER BY
  sp_ereport.time_collected DESC
LIMIT
  $3
