// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`CrucibleDataset`]s.

use super::DataStore;
use super::SQL_BATCH_SIZE;
use crate::authz;
use crate::context::OpContext;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::collection_insert::DatastoreCollection;
use crate::db::identity::Asset;
use crate::db::model::CrucibleDataset;
use crate::db::model::PhysicalDisk;
use crate::db::model::PhysicalDiskPolicy;
use crate::db::model::Zpool;
use crate::db::model::to_db_typed_uuid;
use crate::db::pagination::Paginator;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use diesel::upsert::excluded;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::TransactionError;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_errors::retryable;
use nexus_db_lookup::DbConnection;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::GenericUuid;
use uuid::Uuid;

impl DataStore {
    pub async fn crucible_dataset_get(
        &self,
        dataset_id: DatasetUuid,
    ) -> LookupResult<CrucibleDataset> {
        use nexus_db_schema::schema::crucible_dataset::dsl;

        dsl::crucible_dataset
            .filter(dsl::id.eq(to_db_typed_uuid(dataset_id)))
            .select(CrucibleDataset::as_select())
            .first_async::<CrucibleDataset>(
                &*self.pool_connection_unauthorized().await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Stores a new dataset in the database.
    pub async fn crucible_dataset_upsert(
        &self,
        dataset: CrucibleDataset,
    ) -> CreateResult<CrucibleDataset> {
        let conn = &*self.pool_connection_unauthorized().await?;
        Self::crucible_dataset_upsert_on_connection(&conn, dataset)
            .await
            .map_err(|e| match e {
                TransactionError::CustomError(e) => e,
                TransactionError::Database(e) => {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            })
    }

    async fn crucible_dataset_upsert_on_connection(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        dataset: CrucibleDataset,
    ) -> Result<CrucibleDataset, TransactionError<Error>> {
        use nexus_db_schema::schema::crucible_dataset::dsl;

        let dataset_id = dataset.id();
        let zpool_id = dataset.pool_id;
        Zpool::insert_resource(
            zpool_id,
            diesel::insert_into(dsl::crucible_dataset)
                .values(dataset)
                .on_conflict(dsl::id)
                .do_update()
                .set((
                    dsl::time_modified.eq(Utc::now()),
                    dsl::pool_id.eq(excluded(dsl::pool_id)),
                    dsl::ip.eq(excluded(dsl::ip)),
                    dsl::port.eq(excluded(dsl::port)),
                )),
        )
        .insert_and_get_result_async(conn)
        .await
        .map_err(|e| match e {
            AsyncInsertError::CollectionNotFound => {
                TransactionError::CustomError(Error::ObjectNotFound {
                    type_name: ResourceType::Zpool,
                    lookup_type: LookupType::ById(zpool_id),
                })
            }
            AsyncInsertError::DatabaseError(e) => {
                if retryable(&e) {
                    return TransactionError::Database(e);
                }
                TransactionError::CustomError(public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::Dataset,
                        &dataset_id.to_string(),
                    ),
                ))
            }
        })
    }

    /// Stores a new dataset in the database, but only if a dataset with the
    /// given `id` does not already exist
    ///
    /// Does not update existing rows. If a dataset with the given ID already
    /// exists, returns `Ok(None)`.
    pub async fn crucible_dataset_insert_if_not_exists(
        &self,
        dataset: CrucibleDataset,
    ) -> CreateResult<Option<CrucibleDataset>> {
        use nexus_db_schema::schema::crucible_dataset::dsl;

        let zpool_id = dataset.pool_id;
        Zpool::insert_resource(
            zpool_id,
            diesel::insert_into(dsl::crucible_dataset)
                .values(dataset)
                .on_conflict(dsl::id)
                .do_nothing(),
        )
        .insert_and_get_optional_result_async(
            &*self.pool_connection_unauthorized().await?,
        )
        .await
        .map_err(|e| match e {
            AsyncInsertError::CollectionNotFound => Error::ObjectNotFound {
                type_name: ResourceType::Zpool,
                lookup_type: LookupType::ById(zpool_id),
            },
            AsyncInsertError::DatabaseError(e) => {
                public_error_from_diesel(e, ErrorHandler::Server)
            }
        })
    }

    /// List one page of datasets
    async fn crucible_dataset_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<CrucibleDataset> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        use nexus_db_schema::schema::crucible_dataset::dsl;

        paginated(dsl::crucible_dataset, dsl::id, pagparams)
            .filter(dsl::time_deleted.is_null())
            .select(CrucibleDataset::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// List all Crucible datasets, making as many queries as needed to get them
    /// all
    ///
    /// This should generally not be used in API handlers or other
    /// latency-sensitive contexts, but it can make sense in saga actions or
    /// background tasks.
    pub async fn crucible_dataset_list_all_batched(
        &self,
        opctx: &OpContext,
    ) -> ListResultVec<CrucibleDataset> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        opctx.check_complex_operations_allowed()?;

        let mut all_datasets = Vec::new();
        let mut paginator = Paginator::new(
            SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        while let Some(p) = paginator.next() {
            let batch = self
                .crucible_dataset_list(opctx, &p.current_pagparams())
                .await?;
            paginator = p.found_batch(
                &batch,
                &|d: &nexus_db_model::CrucibleDataset| {
                    d.id().into_untyped_uuid()
                },
            );
            all_datasets.extend(batch);
        }

        Ok(all_datasets)
    }

    pub async fn crucible_dataset_physical_disk_in_service(
        &self,
        dataset_id: DatasetUuid,
    ) -> LookupResult<bool> {
        let conn = self.pool_connection_unauthorized().await?;

        let dataset = {
            use nexus_db_schema::schema::crucible_dataset::dsl;

            dsl::crucible_dataset
                .filter(dsl::id.eq(to_db_typed_uuid(dataset_id)))
                .select(CrucibleDataset::as_select())
                .first_async::<CrucibleDataset>(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?
        };

        let zpool = {
            use nexus_db_schema::schema::zpool::dsl;

            dsl::zpool
                .filter(dsl::id.eq(dataset.pool_id))
                .select(Zpool::as_select())
                .first_async::<Zpool>(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?
        };

        let physical_disk = {
            use nexus_db_schema::schema::physical_disk::dsl;

            dsl::physical_disk
                .filter(dsl::id.eq(zpool.physical_disk_id))
                .select(PhysicalDisk::as_select())
                .first_async::<PhysicalDisk>(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?
        };

        Ok(physical_disk.disk_policy == PhysicalDiskPolicy::InService)
    }

    pub async fn mark_crucible_dataset_not_provisionable(
        &self,
        opctx: &OpContext,
        dataset_id: DatasetUuid,
    ) -> Result<(), Error> {
        let conn = self.pool_connection_authorized(opctx).await?;

        use nexus_db_schema::schema::crucible_dataset::dsl;

        diesel::update(dsl::crucible_dataset)
            .filter(dsl::id.eq(to_db_typed_uuid(dataset_id)))
            .filter(dsl::time_deleted.is_null())
            .set(dsl::no_provision.eq(true))
            .execute_async(&*conn)
            .await
            .map(|_| ())
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn mark_crucible_dataset_provisionable(
        &self,
        opctx: &OpContext,
        dataset_id: DatasetUuid,
    ) -> Result<(), Error> {
        let conn = self.pool_connection_authorized(opctx).await?;

        use nexus_db_schema::schema::crucible_dataset::dsl;

        diesel::update(dsl::crucible_dataset)
            .filter(dsl::id.eq(to_db_typed_uuid(dataset_id)))
            .filter(dsl::time_deleted.is_null())
            .set(dsl::no_provision.eq(false))
            .execute_async(&*conn)
            .await
            .map(|_| ())
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::pub_test_utils::TestDatabase;
    use nexus_db_model::Generation;
    use nexus_db_model::SledBaseboard;
    use nexus_db_model::SledCpuFamily;
    use nexus_db_model::SledSystemHardware;
    use nexus_db_model::SledUpdate;
    use omicron_common::api::external::ByteCount;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::DatasetUuid;
    use omicron_uuid_kinds::PhysicalDiskUuid;
    use omicron_uuid_kinds::SledUuid;
    use omicron_uuid_kinds::ZpoolUuid;

    async fn create_sled_and_zpool(
        datastore: &DataStore,
        opctx: &OpContext,
    ) -> (SledUuid, ZpoolUuid) {
        // Create a fake sled that holds our fake zpool.
        let sled_id = SledUuid::new_v4();
        let sled = SledUpdate::new(
            *sled_id.as_untyped_uuid(),
            "[::1]:0".parse().unwrap(),
            0,
            SledBaseboard {
                serial_number: "test-sn".to_string(),
                part_number: "test-pn".to_string(),
                revision: 0,
            },
            SledSystemHardware {
                is_scrimlet: false,
                usable_hardware_threads: 128,
                usable_physical_ram: (64 << 30).try_into().unwrap(),
                reservoir_size: (16 << 30).try_into().unwrap(),
                cpu_family: SledCpuFamily::AmdMilan,
            },
            Uuid::new_v4(),
            Generation::new(),
        );
        datastore.sled_upsert(sled).await.expect("failed to upsert sled");

        // Create a fake zpool that backs our fake datasets.
        let zpool_id = ZpoolUuid::new_v4();
        let zpool = Zpool::new(
            *zpool_id.as_untyped_uuid(),
            *sled_id.as_untyped_uuid(),
            PhysicalDiskUuid::new_v4(),
            ByteCount::from(0).into(),
        );
        datastore
            .zpool_insert(opctx, zpool)
            .await
            .expect("failed to upsert zpool");

        (sled_id, zpool_id)
    }

    #[tokio::test]
    async fn test_insert_if_not_exists() {
        let logctx = dev::test_setup_log("insert_if_not_exists");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // There should be no datasets initially.
        assert_eq!(
            datastore.crucible_dataset_list_all_batched(opctx).await.unwrap(),
            []
        );

        let (_sled_id, zpool_id) =
            create_sled_and_zpool(&datastore, opctx).await;

        // Inserting a new dataset should succeed.
        let dataset1 = datastore
            .crucible_dataset_insert_if_not_exists(CrucibleDataset::new(
                DatasetUuid::new_v4(),
                *zpool_id.as_untyped_uuid(),
                "[::1]:0".parse().unwrap(),
            ))
            .await
            .expect("failed to insert dataset")
            .expect("insert found unexpected existing dataset");
        let mut expected_datasets = vec![dataset1.clone()];
        assert_eq!(
            datastore.crucible_dataset_list_all_batched(opctx).await.unwrap(),
            expected_datasets,
        );
        assert_eq!(
            datastore.crucible_dataset_list_all_batched(opctx).await.unwrap(),
            expected_datasets,
        );

        // Attempting to insert another dataset with the same ID should succeed
        // without updating the existing record. We'll check this by passing a
        // different socket address.
        let insert_again_result = datastore
            .crucible_dataset_insert_if_not_exists(CrucibleDataset::new(
                dataset1.id(),
                *zpool_id.as_untyped_uuid(),
                "[::1]:12345".parse().unwrap(),
            ))
            .await
            .expect("failed to do-nothing insert dataset");
        assert_eq!(insert_again_result, None);
        assert_eq!(
            datastore.crucible_dataset_list_all_batched(opctx).await.unwrap(),
            expected_datasets,
        );

        // We can can also upsert a different dataset...
        let dataset2 = datastore
            .crucible_dataset_upsert(CrucibleDataset::new(
                DatasetUuid::new_v4(),
                *zpool_id.as_untyped_uuid(),
                "[::1]:0".parse().unwrap(),
            ))
            .await
            .expect("failed to upsert dataset");
        expected_datasets.push(dataset2.clone());
        expected_datasets.sort_by_key(|d| d.id());
        assert_eq!(
            datastore.crucible_dataset_list_all_batched(opctx).await.unwrap(),
            expected_datasets,
        );

        // ... and trying to `insert_if_not_exists` should similarly return
        // `None`.
        let insert_again_result = datastore
            .crucible_dataset_insert_if_not_exists(CrucibleDataset::new(
                dataset1.id(),
                *zpool_id.as_untyped_uuid(),
                "[::1]:12345".parse().unwrap(),
            ))
            .await
            .expect("failed to do-nothing insert dataset");
        assert_eq!(insert_again_result, None);
        assert_eq!(
            datastore.crucible_dataset_list_all_batched(opctx).await.unwrap(),
            expected_datasets,
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
