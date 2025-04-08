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
                1
            )
            = 0
            AND CAST(
                IF(
                  (
                    $5 = 0
                    OR (SELECT quotas.cpus FROM quotas LIMIT 1)
                      >= (
                          (SELECT silo_provisioned.cpus_provisioned FROM silo_provisioned LIMIT 1)
                          + $6
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
                  $7 = 0
                  OR (SELECT quotas.memory FROM quotas LIMIT 1)
                    >= (
                        (SELECT silo_provisioned.ram_provisioned FROM silo_provisioned LIMIT 1) + $8
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
                $9 = 0
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
                      + $10
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
        ($11, DEFAULT, $12, $13, $14, $15)
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
        cpus_provisioned = virtual_provisioning_collection.cpus_provisioned + $16,
        ram_provisioned = virtual_provisioning_collection.ram_provisioned + $17
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
