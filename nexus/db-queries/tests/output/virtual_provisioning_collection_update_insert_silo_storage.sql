WITH
  all_collections AS ((SELECT $1 AS id) UNION (SELECT $2 AS id)),
  quotas
    AS (
      SELECT
        silo_quotas.silo_id,
        silo_quotas.cpus,
        silo_quotas.memory_bytes AS memory,
        silo_quotas.storage_bytes AS storage
      FROM
        silo_quotas
      WHERE
        silo_quotas.silo_id = $3
    ),
  silo_provisioned
    AS (
      SELECT
        virtual_provisioning_collection.id,
        virtual_provisioning_collection.cpus_provisioned,
        virtual_provisioning_collection.ram_provisioned,
        virtual_provisioning_collection.virtual_disk_bytes_provisioned
      FROM
        virtual_provisioning_collection
      WHERE
        virtual_provisioning_collection.id = $4
    ),
  do_update
    AS (
      SELECT
        (
          SELECT
            count(*)
          FROM
            virtual_provisioning_resource
          WHERE
            virtual_provisioning_resource.id = $5
          LIMIT
            1
        )
        = 0
        AND CAST(
            IF(
              (
                $6 = 0
                OR (SELECT quotas.storage FROM quotas LIMIT 1)
                  >= (
                      (
                        SELECT
                          silo_provisioned.virtual_disk_bytes_provisioned
                        FROM
                          silo_provisioned
                        LIMIT
                          1
                      )
                      + $7
                    )
              ),
              'TRUE',
              'Not enough storage'
            )
              AS BOOL
          )
          AS update
    ),
  unused_cte_arm
    AS (
      INSERT
      INTO
        virtual_provisioning_resource
          (
            id,
            time_modified,
            resource_type,
            virtual_disk_bytes_provisioned,
            cpus_provisioned,
            ram_provisioned
          )
      VALUES
        ($8, DEFAULT, $9, $10, $11, $12)
      ON CONFLICT
      DO
        NOTHING
      RETURNING
        virtual_provisioning_resource.id,
        virtual_provisioning_resource.time_modified,
        virtual_provisioning_resource.resource_type,
        virtual_provisioning_resource.virtual_disk_bytes_provisioned,
        virtual_provisioning_resource.cpus_provisioned,
        virtual_provisioning_resource.ram_provisioned
    ),
  virtual_provisioning_collection
    AS (
      UPDATE
        virtual_provisioning_collection
      SET
        time_modified = current_timestamp(),
        virtual_disk_bytes_provisioned
          = virtual_provisioning_collection.virtual_disk_bytes_provisioned + $13
      WHERE
        virtual_provisioning_collection.id = ANY (SELECT all_collections.id FROM all_collections)
        AND (SELECT do_update.update FROM do_update LIMIT 1)
      RETURNING
        virtual_provisioning_collection.id,
        virtual_provisioning_collection.time_modified,
        virtual_provisioning_collection.collection_type,
        virtual_provisioning_collection.virtual_disk_bytes_provisioned,
        virtual_provisioning_collection.cpus_provisioned,
        virtual_provisioning_collection.ram_provisioned
    )
SELECT
  virtual_provisioning_collection.id,
  virtual_provisioning_collection.time_modified,
  virtual_provisioning_collection.collection_type,
  virtual_provisioning_collection.virtual_disk_bytes_provisioned,
  virtual_provisioning_collection.cpus_provisioned,
  virtual_provisioning_collection.ram_provisioned
FROM
  virtual_provisioning_collection
