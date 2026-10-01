SELECT
  vmm.id
FROM
  vmm
WHERE
  (
    ((vmm.time_deleted IS NULL) AND (vmm.stop_for_update_disposition_generation IS NULL))
    AND vmm.state = ANY ($1)
  )
  AND vmm.sled_id = ANY ($2)
LIMIT
  $3
