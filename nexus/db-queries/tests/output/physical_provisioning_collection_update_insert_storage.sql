WITH
  parent_silo AS (SELECT project.silo_id AS id FROM project WHERE project.id = $1),
  all_collections
    AS (
      ((SELECT $2 AS id) UNION (SELECT parent_silo.id AS id FROM parent_silo))
      UNION (SELECT $3 AS id)
    ),
  quotas
    AS (
      SELECT
        silo_quotas.silo_id,
        silo_quotas.cpus,
        silo_quotas.memory_bytes AS memory,
        silo_quotas.storage_bytes AS storage,
        silo_quotas.physical_storage_bytes AS physical_storage
      FROM
        silo_quotas INNER JOIN parent_silo ON silo_quotas.silo_id = parent_silo.id
    ),
  silo_provisioned
    AS (
      SELECT
        physical_provisioning_collection.id,
        physical_provisioning_collection.cpus_provisioned,
        physical_provisioning_collection.ram_provisioned,
        physical_provisioning_collection.physical_writable_disk_bytes,
        physical_provisioning_collection.physical_zfs_snapshot_bytes,
        physical_provisioning_collection.physical_read_only_disk_bytes
      FROM
        physical_provisioning_collection
        INNER JOIN parent_silo ON physical_provisioning_collection.id = parent_silo.id
    ),
  do_update
    AS (
      SELECT
        (
          SELECT
            count(*)
          FROM
            physical_provisioning_resource
          WHERE
            physical_provisioning_resource.id = $4
          LIMIT
            1
        )
        = 0
        AND CAST(
            IF(
              (
                (SELECT quotas.physical_storage FROM quotas LIMIT 1) IS NULL
                OR (SELECT quotas.physical_storage FROM quotas LIMIT 1)
                  >= (
                      (
                        SELECT
                          silo_provisioned.physical_writable_disk_bytes
                          + silo_provisioned.physical_zfs_snapshot_bytes
                          + silo_provisioned.physical_read_only_disk_bytes
                        FROM
                          silo_provisioned
                        LIMIT
                          1
                      )
                      + $5
                      + $6
                      + $7
                    )
              ),
              'TRUE',
              'Not enough physical storage'
            )
              AS BOOL
          )
          AS update
    ),
  unused_cte_arm
    AS (
      INSERT
      INTO
        physical_provisioning_resource
          (
            id,
            time_modified,
            resource_type,
            physical_writable_disk_bytes,
            physical_zfs_snapshot_bytes,
            physical_read_only_disk_bytes,
            cpus_provisioned,
            ram_provisioned
          )
      VALUES
        ($8, DEFAULT, $9, $10, $11, $12, $13, $14)
      ON CONFLICT
      DO
        NOTHING
      RETURNING
        physical_provisioning_resource.id,
        physical_provisioning_resource.time_modified,
        physical_provisioning_resource.resource_type,
        physical_provisioning_resource.physical_writable_disk_bytes,
        physical_provisioning_resource.physical_zfs_snapshot_bytes,
        physical_provisioning_resource.physical_read_only_disk_bytes,
        physical_provisioning_resource.cpus_provisioned,
        physical_provisioning_resource.ram_provisioned
    ),
  physical_provisioning_collection
    AS (
      UPDATE
        physical_provisioning_collection
      SET
        time_modified = current_timestamp(),
        physical_writable_disk_bytes
          = physical_provisioning_collection.physical_writable_disk_bytes
          + CASE WHEN physical_provisioning_collection.id = $15 THEN $16 ELSE $17 END,
        physical_zfs_snapshot_bytes
          = physical_provisioning_collection.physical_zfs_snapshot_bytes
          + CASE WHEN physical_provisioning_collection.id = $18 THEN $19 ELSE $20 END,
        physical_read_only_disk_bytes
          = physical_provisioning_collection.physical_read_only_disk_bytes
          + CASE WHEN physical_provisioning_collection.id = $21 THEN $22 ELSE $23 END
      WHERE
        physical_provisioning_collection.id = ANY (SELECT all_collections.id FROM all_collections)
        AND (SELECT do_update.update FROM do_update LIMIT 1)
      RETURNING
        physical_provisioning_collection.id,
        physical_provisioning_collection.time_modified,
        physical_provisioning_collection.collection_type,
        physical_provisioning_collection.physical_writable_disk_bytes,
        physical_provisioning_collection.physical_zfs_snapshot_bytes,
        physical_provisioning_collection.physical_read_only_disk_bytes,
        physical_provisioning_collection.cpus_provisioned,
        physical_provisioning_collection.ram_provisioned
    )
SELECT
  physical_provisioning_collection.id,
  physical_provisioning_collection.time_modified,
  physical_provisioning_collection.collection_type,
  physical_provisioning_collection.physical_writable_disk_bytes,
  physical_provisioning_collection.physical_zfs_snapshot_bytes,
  physical_provisioning_collection.physical_read_only_disk_bytes,
  physical_provisioning_collection.cpus_provisioned,
  physical_provisioning_collection.ram_provisioned
FROM
  physical_provisioning_collection
