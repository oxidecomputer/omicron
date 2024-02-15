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
          AS update
    ),
  unused_cte_arm
    AS (
      DELETE FROM
        virtual_provisioning_resource
      WHERE
        virtual_provisioning_resource.id = $7
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
          = virtual_provisioning_collection.virtual_disk_bytes_provisioned - $8
      WHERE
        virtual_provisioning_collection.id = ANY (SELECT all_collections.id FROM all_collections)
        AND (SELECT do_update.update FROM do_update LIMIT $9)
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
