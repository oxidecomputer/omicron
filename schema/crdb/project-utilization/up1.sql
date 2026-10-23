CREATE VIEW IF NOT EXISTS omicron.public.project_utilization
AS SELECT
    p.id AS project_id,
    p.name AS project_name,
    s.silo_id AS silo_id,
    s.silo_name AS silo_name,
    s.cpus_allocated AS silo_cpus_allocated,
    s.memory_allocated AS silo_memory_allocated,
    s.storage_allocated AS silo_storage_allocated,
    c.cpus_provisioned AS cpus_provisioned,
    c.ram_provisioned AS memory_provisioned,
    c.virtual_disk_bytes_provisioned AS storage_provisioned,
    s.silo_discoverable as silo_discoverable
FROM
    omicron.public.virtual_provisioning_collection AS c
    INNER JOIN omicron.public.project AS p
    ON c.id = p.id
    INNER JOIN omicron.public.silo_utilization AS s
    ON p.silo_id = s.silo_id
WHERE
    c.collection_type = 'Project'
AND
    p.time_deleted IS NULL;
