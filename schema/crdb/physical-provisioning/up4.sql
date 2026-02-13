-- Add physical provisioning columns to the silo_utilization view.

DROP VIEW IF EXISTS omicron.public.silo_utilization;
CREATE VIEW omicron.public.silo_utilization AS SELECT
    vc.id AS silo_id,
    s.name AS silo_name,
    vc.cpus_provisioned AS cpus_provisioned,
    vc.ram_provisioned AS memory_provisioned,
    vc.virtual_disk_bytes_provisioned AS storage_provisioned,
    q.cpus AS cpus_allocated,
    q.memory_bytes AS memory_allocated,
    q.storage_bytes AS storage_allocated,
    s.discoverable AS silo_discoverable,
    COALESCE(pc.physical_writable_disk_bytes
           + pc.physical_zfs_snapshot_bytes
           + pc.physical_read_only_disk_bytes, 0)
        AS physical_disk_bytes_provisioned,
    q.physical_storage_bytes AS physical_storage_allocated
FROM
    omicron.public.virtual_provisioning_collection AS vc
    RIGHT JOIN omicron.public.silo_quotas AS q ON vc.id = q.silo_id
    INNER JOIN omicron.public.silo AS s ON vc.id = s.id
    LEFT JOIN omicron.public.physical_provisioning_collection AS pc ON vc.id = pc.id
WHERE
    vc.collection_type = 'Silo'
AND
    s.time_deleted IS NULL;
