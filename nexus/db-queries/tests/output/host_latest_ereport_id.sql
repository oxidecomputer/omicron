SELECT
  host_ereport.restart_id, host_ereport.ena
FROM
  host_ereport
WHERE
  host_ereport.sled_id = $1
ORDER BY
  host_ereport.time_collected DESC, host_ereport.ena DESC
LIMIT
  $2
