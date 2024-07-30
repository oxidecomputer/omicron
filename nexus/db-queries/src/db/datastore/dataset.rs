// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Dataset`]s.

use super::DataStore;
use super::SQL_BATCH_SIZE;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::collection_insert::DatastoreCollection;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::identity::Asset;
use crate::db::model::Dataset;
use crate::db::model::PhysicalDisk;
use crate::db::model::PhysicalDiskPolicy;
use crate::db::model::Zpool;
use crate::db::pagination::paginated;
use crate::db::pagination::Paginator;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use diesel::upsert::excluded;
use nexus_db_model::DatasetKind;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use uuid::Uuid;

impl DataStore {
    pub async fn dataset_get(&self, dataset_id: Uuid) -> LookupResult<Dataset> {
        use db::schema::dataset::dsl;

        dsl::dataset
            .filter(dsl::id.eq(dataset_id))
            .select(Dataset::as_select())
            .first_async::<Dataset>(
                &*self.pool_connection_unauthorized().await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Stores a new dataset in the database.
    pub async fn dataset_upsert(
        &self,
        dataset: Dataset,
    ) -> CreateResult<Dataset> {
        use db::schema::dataset::dsl;

        let dataset_id = dataset.id();
        let zpool_id = dataset.pool_id;
        Zpool::insert_resource(
            zpool_id,
            diesel::insert_into(dsl::dataset)
                .values(dataset)
                .on_conflict(dsl::id)
                .do_update()
                .set((
                    dsl::time_modified.eq(Utc::now()),
                    dsl::pool_id.eq(excluded(dsl::pool_id)),
                    dsl::ip.eq(excluded(dsl::ip)),
                    dsl::port.eq(excluded(dsl::port)),
                    dsl::kind.eq(excluded(dsl::kind)),
                )),
        )
        .insert_and_get_result_async(
            &*self.pool_connection_unauthorized().await?,
        )
        .await
        .map_err(|e| match e {
            AsyncInsertError::CollectionNotFound => Error::ObjectNotFound {
                type_name: ResourceType::Zpool,
                lookup_type: LookupType::ById(zpool_id),
            },
            AsyncInsertError::DatabaseError(e) => public_error_from_diesel(
                e,
                ErrorHandler::Conflict(
                    ResourceType::Dataset,
                    &dataset_id.to_string(),
                ),
            ),
        })
    }

    /// Stores a new dataset in the database, but only if a dataset with the
    /// given `id` does not already exist
    ///
    /// Does not update existing rows. If a dataset with the given ID already
    /// exists, returns `Ok(None)`.
    pub async fn dataset_insert_if_not_exists(
        &self,
        dataset: Dataset,
    ) -> CreateResult<Option<Dataset>> {
        use db::schema::dataset::dsl;

        let zpool_id = dataset.pool_id;
        Zpool::insert_resource(
            zpool_id,
            diesel::insert_into(dsl::dataset)
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
    ///
    /// If `filter_kind` is `Some(value)`, only datasets with a `kind` matching
    /// `value` will be returned. If `filter_kind` is `None`, all datasets will
    /// be returned.
    async fn dataset_list(
        &self,
        opctx: &OpContext,
        filter_kind: Option<DatasetKind>,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<Dataset> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        use db::schema::dataset::dsl;

        let mut query = paginated(dsl::dataset, dsl::id, pagparams)
            .filter(dsl::time_deleted.is_null());

        if let Some(kind) = filter_kind {
            query = query.filter(dsl::kind.eq(kind));
        }

        query
            .select(Dataset::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// List all datasets, making as many queries as needed to get them all
    ///
    /// If `filter_kind` is `Some(value)`, only datasets with a `kind` matching
    /// `value` will be returned. If `filter_kind` is `None`, all datasets will
    /// be returned.
    ///
    /// This should generally not be used in API handlers or other
    /// latency-sensitive contexts, but it can make sense in saga actions or
    /// background tasks.
    pub async fn dataset_list_all_batched(
        &self,
        opctx: &OpContext,
        filter_kind: Option<DatasetKind>,
    ) -> ListResultVec<Dataset> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        opctx.check_complex_operations_allowed()?;

        let mut all_datasets = Vec::new();
        let mut paginator = Paginator::new(SQL_BATCH_SIZE);
        while let Some(p) = paginator.next() {
            let batch = self
                .dataset_list(opctx, filter_kind, &p.current_pagparams())
                .await?;
            paginator =
                p.found_batch(&batch, &|d: &nexus_db_model::Dataset| d.id());
            all_datasets.extend(batch);
        }

        Ok(all_datasets)
    }

    pub async fn dataset_physical_disk_in_service(
        &self,
        dataset_id: Uuid,
    ) -> LookupResult<bool> {
        let conn = self.pool_connection_unauthorized().await?;

        let dataset = {
            use db::schema::dataset::dsl;

            dsl::dataset
                .filter(dsl::id.eq(dataset_id))
                .select(Dataset::as_select())
                .first_async::<Dataset>(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?
        };

        let zpool = {
            use db::schema::zpool::dsl;

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
            use db::schema::physical_disk::dsl;

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
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::datastore::test_utils::datastore_test;
    use nexus_db_model::Generation;
    use nexus_db_model::SledBaseboard;
    use nexus_db_model::SledSystemHardware;
    use nexus_db_model::SledUpdate;
    use nexus_test_utils::db::test_setup_database;
    use omicron_test_utils::dev;

    #[tokio::test]
    async fn test_insert_if_not_exists() {
        let logctx = dev::test_setup_log("inventory_insert");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;
        let opctx = &opctx;

        // There should be no datasets initially.
        assert_eq!(
            datastore.dataset_list_all_batched(opctx, None).await.unwrap(),
            []
        );

        // Create a fake sled that holds our fake zpool.
        let sled_id = Uuid::new_v4();
        let sled = SledUpdate::new(
            sled_id,
            "[::1]:0".parse().unwrap(),
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
            },
            Uuid::new_v4(),
            Generation::new(),
        );
        datastore.sled_upsert(sled).await.expect("failed to upsert sled");

        // Create a fake zpool that backs our fake datasets.
        let zpool_id = Uuid::new_v4();
        let zpool = Zpool::new(zpool_id, sled_id, Uuid::new_v4());
        datastore
            .zpool_insert(opctx, zpool)
            .await
            .expect("failed to upsert zpool");

        // Inserting a new dataset should succeed.
        let dataset1 = datastore
            .dataset_insert_if_not_exists(Dataset::new(
                Uuid::new_v4(),
                zpool_id,
                Some("[::1]:0".parse().unwrap()),
                DatasetKind::Crucible,
                None,
            ))
            .await
            .expect("failed to insert dataset")
            .expect("insert found unexpected existing dataset");
        let mut expected_datasets = vec![dataset1.clone()];
        assert_eq!(
            datastore.dataset_list_all_batched(opctx, None).await.unwrap(),
            expected_datasets,
        );
        assert_eq!(
            datastore
                .dataset_list_all_batched(opctx, Some(DatasetKind::Crucible))
                .await
                .unwrap(),
            expected_datasets,
        );
        assert_eq!(
            datastore
                .dataset_list_all_batched(opctx, Some(DatasetKind::Cockroach))
                .await
                .unwrap(),
            [],
        );

        // Attempting to insert another dataset with the same ID should succeed
        // without updating the existing record. We'll check this by passing a
        // different socket address and kind.
        let insert_again_result = datastore
            .dataset_insert_if_not_exists(Dataset::new(
                dataset1.id(),
                zpool_id,
                Some("[::1]:12345".parse().unwrap()),
                DatasetKind::Cockroach,
                None,
            ))
            .await
            .expect("failed to do-nothing insert dataset");
        assert_eq!(insert_again_result, None);
        assert_eq!(
            datastore.dataset_list_all_batched(opctx, None).await.unwrap(),
            expected_datasets,
        );

        // We can can also upsert a different dataset...
        let dataset2 = datastore
            .dataset_upsert(Dataset::new(
                Uuid::new_v4(),
                zpool_id,
                Some("[::1]:0".parse().unwrap()),
                DatasetKind::Cockroach,
                None,
            ))
            .await
            .expect("failed to upsert dataset");
        expected_datasets.push(dataset2.clone());
        expected_datasets.sort_by_key(|d| d.id());
        assert_eq!(
            datastore.dataset_list_all_batched(opctx, None).await.unwrap(),
            expected_datasets,
        );
        assert_eq!(
            datastore
                .dataset_list_all_batched(opctx, Some(DatasetKind::Crucible))
                .await
                .unwrap(),
            [dataset1.clone()],
        );
        assert_eq!(
            datastore
                .dataset_list_all_batched(opctx, Some(DatasetKind::Cockroach))
                .await
                .unwrap(),
            [dataset2.clone()],
        );

        // ... and trying to `insert_if_not_exists` should similarly return
        // `None`.
        let insert_again_result = datastore
            .dataset_insert_if_not_exists(Dataset::new(
                dataset1.id(),
                zpool_id,
                Some("[::1]:12345".parse().unwrap()),
                DatasetKind::Cockroach,
                None,
            ))
            .await
            .expect("failed to do-nothing insert dataset");
        assert_eq!(insert_again_result, None);
        assert_eq!(
            datastore.dataset_list_all_batched(opctx, None).await.unwrap(),
            expected_datasets,
        );

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
