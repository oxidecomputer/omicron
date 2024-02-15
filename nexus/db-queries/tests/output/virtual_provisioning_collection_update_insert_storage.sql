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
        silo_quotas.storage_bytes AS storage
      FROM
        silo_quotas INNER JOIN parent_silo ON silo_quotas.silo_id = parent_silo.id
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
        INNER JOIN parent_silo ON virtual_provisioning_collection.id = parent_silo.id
    ),
  do_update
    AS (
      SELECT
        (
          (
            (
              SELECT
                count(*)
              FROM
                virtual_provisioning_resource
              WHERE
                virtual_provisioning_resource.id = $4
              LIMIT
                $5
            )
            = $6
            AND CAST(
                IF(
                  (
                    $7 = $8
                    OR (SELECT quotas.cpus FROM quotas LIMIT $9)
                      >= (
                          (SELECT silo_provisioned.cpus_provisioned FROM silo_provisioned LIMIT $10)
                          + $11
                        )
                  ),
                  'TRUE',
                  'Not enough cpus'
                )
                  AS BOOL
              )
          )
          AND CAST(
              IF(
                (
                  $12 = $13
                  OR (SELECT quotas.memory FROM quotas LIMIT $14)
                    >= (
                        (SELECT silo_provisioned.ram_provisioned FROM silo_provisioned LIMIT $15)
                        + $16
                      )
                ),
                'TRUE',
                'Not enough memory'
              )
                AS BOOL
            )
        )
        AND CAST(
            IF(
              (
                $17 = $18
                OR (SELECT quotas.storage FROM quotas LIMIT $19)
                  >= (
                      (
                        SELECT
                          silo_provisioned.virtual_disk_bytes_provisioned
                        FROM
                          silo_provisioned
                        LIMIT
                          $20
                      )
                      + $21
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
        ($22, DEFAULT, $23, $24, $25, $26)
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
          = virtual_provisioning_collection.virtual_disk_bytes_provisioned + $27
      WHERE
        virtual_provisioning_collection.id = ANY (SELECT all_collections.id FROM all_collections)
        AND (SELECT do_update.update FROM do_update LIMIT $28)
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
