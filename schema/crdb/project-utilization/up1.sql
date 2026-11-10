CREATE VIEW IF NOT EXISTS omicron.public.project_utilization
AS SELECT
    p.id AS project_id,
    s.silo_id AS silo_id,
    c.cpus_provisioned AS cpus_provisioned,
    c.ram_provisioned AS memory_provisioned,
    c.virtual_disk_bytes_provisioned AS storage_provisioned
FROM
    omicron.public.virtual_provisioning_collection AS c
    INNER JOIN omicron.public.project AS p
    ON c.id = p.id
    INNER JOIN omicron.public.silo_utilization AS s
    ON p.silo_id = s.silo_id
WHERE
    c.collection_type = 'Project'
    AND (
        s.silo_discoverable IS TRUE
            OR s.cpus_allocated > 0
            OR s.memory_allocated > 0
            OR s.storage_allocated > 0
    )
    AND p.time_deleted IS NULL;
