CREATE VIEW IF NOT EXISTS omicron.public.silo_utilization AS
SELECT
  c.id AS silo_id,
  s.name AS silo_name,
  c.cpus_provisioned AS cpus_provisioned,
  c.ram_provisioned AS memory_provisioned,
  c.virtual_disk_bytes_provisioned AS storage_provisioned,
  q.cpus AS cpus_allocated,
  q.memory_bytes AS memory_allocated,
  q.storage_bytes AS storage_allocated
FROM
  omicron.public.virtual_provisioning_collection AS c
  RIGHT JOIN omicron.public.silo_quotas AS q ON c.id = q.silo_id
  INNER JOIN omicron.public.silo AS s ON c.id = s.id
WHERE
  c.collection_type = 'Silo'
  AND s.time_deleted IS NULL;