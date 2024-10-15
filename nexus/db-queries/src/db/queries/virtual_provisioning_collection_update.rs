// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of queries for updating resource provisioning info.

use crate::db::column_walker::AllColumnsOf;
use crate::db::model::ByteCount;
use crate::db::model::ResourceTypeProvisioned;
use crate::db::model::VirtualProvisioningCollection;
use crate::db::model::VirtualProvisioningResource;
use crate::db::raw_query_builder::{QueryBuilder, TypedSqlQuery};
use crate::db::schema::virtual_provisioning_collection;
use crate::db::schema::virtual_provisioning_resource;
use crate::db::true_or_cast_error::matches_sentinel;
use async_bb8_diesel::RunError;
use const_format::concatcp;
use diesel::pg::Pg;
use diesel::sql_types;
use omicron_common::api::external;
use omicron_common::api::external::MessagePair;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::InstanceUuid;

type AllColumnsOfVirtualResource =
    AllColumnsOf<virtual_provisioning_resource::table>;
type AllColumnsOfVirtualCollection =
    AllColumnsOf<virtual_provisioning_collection::table>;

const NOT_ENOUGH_CPUS_SENTINEL: &'static str = "Not enough cpus";
const NOT_ENOUGH_MEMORY_SENTINEL: &'static str = "Not enough memory";
const NOT_ENOUGH_STORAGE_SENTINEL: &'static str = "Not enough storage";

/// Translates a generic pool error to an external error based
/// on messages which may be emitted when provisioning virtual resources
/// such as instances and disks.
pub fn from_diesel(e: RunError) -> external::Error {
    use crate::db::error;

    let sentinels = [
        NOT_ENOUGH_CPUS_SENTINEL,
        NOT_ENOUGH_MEMORY_SENTINEL,
        NOT_ENOUGH_STORAGE_SENTINEL,
    ];
    if let Some(sentinel) = matches_sentinel(&e, &sentinels) {
        match sentinel {
            NOT_ENOUGH_CPUS_SENTINEL => {
                return external::Error::InsufficientCapacity {
                    message: MessagePair::new_full(
                         "vCPU Limit Exceeded: Not enough vCPUs to complete request. Either stop unused instances to free up resources or contact the rack operator to request a capacity increase.".to_string(),
                         "User tried to allocate an instance but the virtual provisioning resource table indicated that there were not enough CPUs available to satisfy the request.".to_string(),
                    )
                }
            }
            NOT_ENOUGH_MEMORY_SENTINEL => {
                return external::Error::InsufficientCapacity {
                    message: MessagePair::new_full(
                         "Memory Limit Exceeded: Not enough memory to complete request. Either stop unused instances to free up resources or contact the rack operator to request a capacity increase.".to_string(),
                         "User tried to allocate an instance but the virtual provisioning resource table indicated that there were not enough RAM available to satisfy the request.".to_string(),
                    )
                }
            }
            NOT_ENOUGH_STORAGE_SENTINEL => {
                return external::Error::InsufficientCapacity {
                    message: MessagePair::new_full(
                         "Storage Limit Exceeded: Not enough storage to complete request. Either remove unneeded disks and snapshots to free up resources or contact the rack operator to request a capacity increase.".to_string(),
                         "User tried to allocate a disk or snapshot but the virtual provisioning resource table indicated that there were not enough storage available to satisfy the request.".to_string(),
                    )
                }
            }
            _ => {}
        }
    }
    error::public_error_from_diesel(e, error::ErrorHandler::Server)
}

/// The virtual resource collection is only updated when a resource is inserted
/// or deleted from the resource provisioning table. By probing for the presence
/// or absence of a resource, we can update collections at the same time as we
/// create or destroy the resource, which helps make the operation idempotent.
#[derive(Clone)]
enum UpdateKind {
    InsertStorage(VirtualProvisioningResource),
    DeleteStorage { id: uuid::Uuid, disk_byte_diff: ByteCount },
    InsertInstance(VirtualProvisioningResource),
    DeleteInstance { id: uuid::Uuid, cpus_diff: i64, ram_diff: ByteCount },
}

type SelectableSql<T> = <
    <T as diesel::Selectable<Pg>>::SelectExpression as diesel::Expression
>::SqlType;

/// Constructs a CTE for updating resource provisioning information in all
/// collections for a particular object.
pub struct VirtualProvisioningCollectionUpdate {}

impl VirtualProvisioningCollectionUpdate {
    // Generic utility for updating all collections including this resource,
    // even transitively.
    //
    // Propagated updates include:
    // - Project
    // - Silo
    // - Fleet
    fn apply_update(
        update_kind: UpdateKind,
        project_id: uuid::Uuid,
    ) -> TypedSqlQuery<SelectableSql<VirtualProvisioningCollection>> {
        let query = QueryBuilder::new().sql("
WITH
  parent_silo AS (SELECT project.silo_id AS id FROM project WHERE project.id = ").param().sql("),")
            .bind::<sql_types::Uuid, _>(project_id).sql("
  all_collections
    AS (
      ((SELECT ").param().sql(" AS id) UNION (SELECT parent_silo.id AS id FROM parent_silo))
      UNION (SELECT ").param().sql(" AS id)
    ),")
            .bind::<sql_types::Uuid, _>(project_id)
            .bind::<sql_types::Uuid, _>(*nexus_db_fixed_data::FLEET_ID)
            .sql("
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
    ),");

        let query = match update_kind.clone() {
            UpdateKind::InsertInstance(resource) | UpdateKind::InsertStorage(resource) => {
                query.sql("
  do_update
    AS (
      SELECT
        (
          (
            (
              SELECT count(*)
              FROM virtual_provisioning_resource
              WHERE virtual_provisioning_resource.id = ").param().sql("
              LIMIT 1
            )
            = 0
            AND CAST(
                IF(
                  (
                    ").param().sql(" = 0
                    OR (SELECT quotas.cpus FROM quotas LIMIT 1)
                      >= (
                          (SELECT silo_provisioned.cpus_provisioned FROM silo_provisioned LIMIT 1)
                          + ").param().sql(concatcp!("
                        )
                  ),
                  'TRUE',
                  '", NOT_ENOUGH_CPUS_SENTINEL, "'
                )
                  AS BOOL
              )
          )
          AND CAST(
              IF(
                (
                  ")).param().sql(" = 0
                  OR (SELECT quotas.memory FROM quotas LIMIT 1)
                    >= (
                        (SELECT silo_provisioned.ram_provisioned FROM silo_provisioned LIMIT 1)
                        + ").param().sql(concatcp!("
                      )
                ),
                'TRUE',
                '", NOT_ENOUGH_MEMORY_SENTINEL, "'
              )
                AS BOOL
            )
        )
        AND CAST(
            IF(
              (
                ")).param().sql(" = 0
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
                      + ").param().sql(concatcp!("
                    )
              ),
              'TRUE',
              '", NOT_ENOUGH_STORAGE_SENTINEL, "'
            )
              AS BOOL
          )
          AS update
    ),"))
                .bind::<sql_types::Uuid, _>(resource.id)
                .bind::<sql_types::BigInt, _>(resource.cpus_provisioned)
                .bind::<sql_types::BigInt, _>(resource.cpus_provisioned)
                .bind::<sql_types::BigInt, _>(resource.ram_provisioned)
                .bind::<sql_types::BigInt, _>(resource.ram_provisioned)
                .bind::<sql_types::BigInt, _>(resource.virtual_disk_bytes_provisioned)
                .bind::<sql_types::BigInt, _>(resource.virtual_disk_bytes_provisioned)
            },
            UpdateKind::DeleteStorage { id, .. } => {
                query.sql("
  do_update
    AS (
      SELECT
        (
          SELECT
            count(*)
          FROM
            virtual_provisioning_resource
          WHERE
            virtual_provisioning_resource.id = ").param().sql("
          LIMIT
            1
        ) = 1
          AS update
    ),")
                .bind::<sql_types::Uuid, _>(id)
            },
            UpdateKind::DeleteInstance { id, .. } => {
                // If the relevant instance ID is not in the database, then some
                // other operation must have ensured the instance was previously
                // stopped (because that's the only way it could have been deleted),
                // and that operation should have cleaned up the resources already,
                // in which case there's nothing to do here.
                query.sql("
  do_update
    AS (
      SELECT
        (
          SELECT
            count(*)
          FROM
            virtual_provisioning_resource
          WHERE
            virtual_provisioning_resource.id = ").param().sql("
          LIMIT
            1
        ) = 1 AND
        EXISTS (
          SELECT 1
          FROM
            instance
          WHERE
            instance.id = ").param().sql("
          LIMIT 1
        )
          AS update
    ),")
                .bind::<sql_types::Uuid, _>(id)
                .bind::<sql_types::Uuid, _>(id)
            },
        };

        let query = match update_kind.clone() {
            UpdateKind::InsertInstance(resource)
            | UpdateKind::InsertStorage(resource) => query
                .sql(
                    "
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
        (",
                )
                .param()
                .sql(", DEFAULT, ")
                .param()
                .sql(", ")
                .param()
                .sql(", ")
                .param()
                .sql(", ")
                .param()
                .sql(
                    ")
      ON CONFLICT
      DO
        NOTHING
      RETURNING ",
                )
                .sql(AllColumnsOfVirtualResource::with_prefix(
                    "virtual_provisioning_resource",
                ))
                .sql("),")
                .bind::<sql_types::Uuid, _>(resource.id)
                .bind::<sql_types::Text, _>(resource.resource_type)
                .bind::<sql_types::BigInt, _>(
                    resource.virtual_disk_bytes_provisioned,
                )
                .bind::<sql_types::BigInt, _>(resource.cpus_provisioned)
                .bind::<sql_types::BigInt, _>(resource.ram_provisioned),
            UpdateKind::DeleteInstance { id, .. }
            | UpdateKind::DeleteStorage { id, .. } => query
                .sql(
                    "
  unused_cte_arm
    AS (
      DELETE FROM
        virtual_provisioning_resource
      WHERE
        virtual_provisioning_resource.id = ",
                )
                .param()
                .sql(
                    "
        AND (SELECT do_update.update FROM do_update LIMIT 1)
      RETURNING ",
                )
                .sql(AllColumnsOfVirtualResource::with_prefix(
                    "virtual_provisioning_resource",
                ))
                .sql("),")
                .bind::<sql_types::Uuid, _>(id),
        };

        let query = query.sql(
            "
  virtual_provisioning_collection
    AS (
      UPDATE
        virtual_provisioning_collection
      SET",
        );
        let query = match update_kind.clone() {
            UpdateKind::InsertInstance(resource) => query
                .sql(
                    "
        time_modified = current_timestamp(),
        cpus_provisioned = virtual_provisioning_collection.cpus_provisioned + ",
                )
                .param()
                .sql(
                    ",
        ram_provisioned = virtual_provisioning_collection.ram_provisioned + ",
                )
                .param()
                .bind::<sql_types::BigInt, _>(resource.cpus_provisioned)
                .bind::<sql_types::BigInt, _>(resource.ram_provisioned),
            UpdateKind::InsertStorage(resource) => query
                .sql(
                    "
        time_modified = current_timestamp(),
        virtual_disk_bytes_provisioned
          = virtual_provisioning_collection.virtual_disk_bytes_provisioned + ",
                )
                .param()
                .bind::<sql_types::BigInt, _>(
                    resource.virtual_disk_bytes_provisioned,
                ),
            UpdateKind::DeleteInstance { cpus_diff, ram_diff, .. } => query
                .sql(
                    "
        time_modified = current_timestamp(),
        cpus_provisioned = virtual_provisioning_collection.cpus_provisioned - ",
                )
                .param()
                .sql(
                    ",
        ram_provisioned = virtual_provisioning_collection.ram_provisioned - ",
                )
                .param()
                .bind::<sql_types::BigInt, _>(cpus_diff)
                .bind::<sql_types::BigInt, _>(ram_diff),
            UpdateKind::DeleteStorage { disk_byte_diff, .. } => query
                .sql(
                    "
        time_modified = current_timestamp(),
        virtual_disk_bytes_provisioned
          = virtual_provisioning_collection.virtual_disk_bytes_provisioned - ",
                )
                .param()
                .bind::<sql_types::BigInt, _>(disk_byte_diff),
        };

        query.sql("
      WHERE
        virtual_provisioning_collection.id = ANY (SELECT all_collections.id FROM all_collections)
        AND (SELECT do_update.update FROM do_update LIMIT 1)
      RETURNING "
        ).sql(AllColumnsOfVirtualCollection::with_prefix("virtual_provisioning_collection")).sql("
    )
SELECT "
    ).sql(AllColumnsOfVirtualCollection::with_prefix("virtual_provisioning_collection")).sql("
FROM
  virtual_provisioning_collection
").query()
    }

    pub fn new_insert_storage(
        id: uuid::Uuid,
        disk_byte_diff: ByteCount,
        project_id: uuid::Uuid,
        storage_type: crate::db::datastore::StorageType,
    ) -> TypedSqlQuery<SelectableSql<VirtualProvisioningCollection>> {
        let mut provision =
            VirtualProvisioningResource::new(id, storage_type.into());
        provision.virtual_disk_bytes_provisioned = disk_byte_diff;

        Self::apply_update(UpdateKind::InsertStorage(provision), project_id)
    }

    pub fn new_delete_storage(
        id: uuid::Uuid,
        disk_byte_diff: ByteCount,
        project_id: uuid::Uuid,
    ) -> TypedSqlQuery<SelectableSql<VirtualProvisioningCollection>> {
        Self::apply_update(
            UpdateKind::DeleteStorage { id, disk_byte_diff },
            project_id,
        )
    }

    pub fn new_insert_instance(
        id: InstanceUuid,
        cpus_diff: i64,
        ram_diff: ByteCount,
        project_id: uuid::Uuid,
    ) -> TypedSqlQuery<SelectableSql<VirtualProvisioningCollection>> {
        let mut provision = VirtualProvisioningResource::new(
            id.into_untyped_uuid(),
            ResourceTypeProvisioned::Instance,
        );
        provision.cpus_provisioned = cpus_diff;
        provision.ram_provisioned = ram_diff;

        Self::apply_update(UpdateKind::InsertInstance(provision), project_id)
    }

    pub fn new_delete_instance(
        id: InstanceUuid,
        cpus_diff: i64,
        ram_diff: ByteCount,
        project_id: uuid::Uuid,
    ) -> TypedSqlQuery<SelectableSql<VirtualProvisioningCollection>> {
        Self::apply_update(
            UpdateKind::DeleteInstance {
                id: id.into_untyped_uuid(),
                cpus_diff,
                ram_diff,
            },
            project_id,
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::explain::ExplainableAsync;
    use crate::db::raw_query_builder::expectorate_query_contents;
    use nexus_test_utils::db::test_setup_database;
    use omicron_test_utils::dev;
    use uuid::Uuid;

    // These tests are a bit of a "change detector", but they're here to help
    // with debugging too. If you change this query, it can be useful to see
    // exactly how the output SQL has been altered.

    #[tokio::test]
    async fn expectorate_query_insert_storage() {
        let id = Uuid::nil();
        let project_id = Uuid::nil();
        let disk_byte_diff = 2048.try_into().unwrap();
        let storage_type = crate::db::datastore::StorageType::Disk;

        let query = VirtualProvisioningCollectionUpdate::new_insert_storage(
            id,
            disk_byte_diff,
            project_id,
            storage_type,
        );
        expectorate_query_contents(
            &query,
            "tests/output/virtual_provisioning_collection_update_insert_storage.sql",
        ).await;
    }

    #[tokio::test]
    async fn expectorate_query_delete_storage() {
        let id = Uuid::nil();
        let project_id = Uuid::nil();
        let disk_byte_diff = 2048.try_into().unwrap();

        let query = VirtualProvisioningCollectionUpdate::new_delete_storage(
            id,
            disk_byte_diff,
            project_id,
        );

        expectorate_query_contents(
            &query,
            "tests/output/virtual_provisioning_collection_update_delete_storage.sql",
        ).await;
    }

    #[tokio::test]
    async fn expectorate_query_insert_instance() {
        let id = InstanceUuid::nil();
        let project_id = Uuid::nil();
        let cpus_diff = 4;
        let ram_diff = 2048.try_into().unwrap();

        let query = VirtualProvisioningCollectionUpdate::new_insert_instance(
            id, cpus_diff, ram_diff, project_id,
        );

        expectorate_query_contents(
            &query,
            "tests/output/virtual_provisioning_collection_update_insert_instance.sql",
        ).await;
    }

    #[tokio::test]
    async fn expectorate_query_delete_instance() {
        let id = InstanceUuid::nil();
        let project_id = Uuid::nil();
        let cpus_diff = 4;
        let ram_diff = 2048.try_into().unwrap();

        let query = VirtualProvisioningCollectionUpdate::new_delete_instance(
            id, cpus_diff, ram_diff, project_id,
        );

        expectorate_query_contents(
            &query,
            "tests/output/virtual_provisioning_collection_update_delete_instance.sql",
        ).await;
    }

    // Explain the possible forms of the SQL query to ensure that it
    // creates a valid SQL string.

    #[tokio::test]
    async fn explain_insert_storage() {
        let logctx = dev::test_setup_log("explain_insert_storage");
        let log = logctx.log.new(o!());
        let mut db = test_setup_database(&log).await;
        let cfg = crate::db::Config { url: db.pg_config().clone() };
        let pool = crate::db::Pool::new_single_host(&logctx.log, &cfg);
        let conn = pool.claim().await.unwrap();

        let id = Uuid::nil();
        let project_id = Uuid::nil();
        let disk_byte_diff = 2048.try_into().unwrap();
        let storage_type = crate::db::datastore::StorageType::Disk;

        let query = VirtualProvisioningCollectionUpdate::new_insert_storage(
            id,
            disk_byte_diff,
            project_id,
            storage_type,
        );
        let _ = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn explain_delete_storage() {
        let logctx = dev::test_setup_log("explain_delete_storage");
        let log = logctx.log.new(o!());
        let mut db = test_setup_database(&log).await;
        let cfg = crate::db::Config { url: db.pg_config().clone() };
        let pool = crate::db::Pool::new_single_host(&logctx.log, &cfg);
        let conn = pool.claim().await.unwrap();

        let id = Uuid::nil();
        let project_id = Uuid::nil();
        let disk_byte_diff = 2048.try_into().unwrap();

        let query = VirtualProvisioningCollectionUpdate::new_delete_storage(
            id,
            disk_byte_diff,
            project_id,
        );
        let _ = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn explain_insert_instance() {
        let logctx = dev::test_setup_log("explain_insert_instance");
        let log = logctx.log.new(o!());
        let mut db = test_setup_database(&log).await;
        let cfg = crate::db::Config { url: db.pg_config().clone() };
        let pool = crate::db::Pool::new_single_host(&logctx.log, &cfg);
        let conn = pool.claim().await.unwrap();

        let id = InstanceUuid::nil();
        let project_id = Uuid::nil();
        let cpus_diff = 16.try_into().unwrap();
        let ram_diff = 2048.try_into().unwrap();

        let query = VirtualProvisioningCollectionUpdate::new_insert_instance(
            id, cpus_diff, ram_diff, project_id,
        );
        let _ = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn explain_delete_instance() {
        let logctx = dev::test_setup_log("explain_delete_instance");
        let log = logctx.log.new(o!());
        let mut db = test_setup_database(&log).await;
        let cfg = crate::db::Config { url: db.pg_config().clone() };
        let pool = crate::db::Pool::new_single_host(&logctx.log, &cfg);
        let conn = pool.claim().await.unwrap();

        let id = InstanceUuid::nil();
        let project_id = Uuid::nil();
        let cpus_diff = 16.try_into().unwrap();
        let ram_diff = 2048.try_into().unwrap();

        let query = VirtualProvisioningCollectionUpdate::new_delete_instance(
            id, cpus_diff, ram_diff, project_id,
        );
        let _ = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
