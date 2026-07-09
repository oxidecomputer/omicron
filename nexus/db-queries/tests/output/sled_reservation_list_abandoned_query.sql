SELECT
  sled_resource_vmm.id,
  sled_resource_vmm.sled_id,
  sled_resource_vmm.hardware_threads,
  sled_resource_vmm.rss_ram,
  sled_resource_vmm.reservoir_ram,
  sled_resource_vmm.instance_id,
  sled_resource_vmm.state
FROM
  sled_resource_vmm LEFT JOIN vmm ON sled_resource_vmm.id = vmm.id
WHERE
  sled_resource_vmm.state = $1
  AND ((vmm.state = ANY ($2) OR (vmm.time_deleted IS NOT NULL)) OR (vmm.state IS NULL))
ORDER BY
  sled_resource_vmm.id ASC
LIMIT
  $3
