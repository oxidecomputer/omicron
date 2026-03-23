// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`RendezvousDebugDataset`]s.

use super::DataStore;
use super::SQL_BATCH_SIZE;
use crate::authz;
use crate::context::OpContext;
use crate::db::pagination::Paginator;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_model::RendezvousDebugDataset;
use nexus_db_model::to_db_typed_uuid;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_uuid_kinds::BlueprintUuid;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::GenericUuid;

impl DataStore {
    /// List one page of debug datasets
    ///
    /// This fetches all debug datasets, including those that have been
    /// tombstoned.
    async fn debug_dataset_list_all_page(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, DatasetUuid>,
    ) -> ListResultVec<RendezvousDebugDataset> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        use nexus_db_schema::schema::rendezvous_debug_dataset::dsl;

        paginated(
            dsl::rendezvous_debug_dataset,
            dsl::id,
            &pagparams.map_name(|id| id.as_untyped_uuid()),
        )
        .select(RendezvousDebugDataset::as_select())
        .load_async(&*self.pool_connection_authorized(opctx).await?)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// List all debug datasets, making as many queries as needed to get them
    /// all
    ///
    /// This fetches all debug datasets, including those that have been
    /// tombstoned.
    ///
    /// This should generally not be used in API handlers or other
    /// latency-sensitive contexts, but it can make sense in saga actions or
    /// background tasks.
    pub async fn debug_dataset_list_all_batched(
        &self,
        opctx: &OpContext,
    ) -> ListResultVec<RendezvousDebugDataset> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        opctx.check_complex_operations_allowed()?;

        let mut all_datasets = Vec::new();
        let mut paginator = Paginator::new(
            SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        while let Some(p) = paginator.next() {
            let batch = self
                .debug_dataset_list_all_page(opctx, &p.current_pagparams())
                .await?;
            paginator =
                p.found_batch(&batch, &|d: &RendezvousDebugDataset| d.id());
            all_datasets.extend(batch);
        }

        Ok(all_datasets)
    }

    /// Insert a new debug dataset record
    ///
    /// If this dataset row already exists, does nothing. In particular:
    ///
    /// * The zpool ID associated with a dataset cannot change
    /// * If the dataset has been tombstoned, this will not "untombstone" it
    pub async fn debug_dataset_insert_if_not_exists(
        &self,
        opctx: &OpContext,
        dataset: RendezvousDebugDataset,
    ) -> CreateResult<Option<RendezvousDebugDataset>> {
        opctx.authorize(authz::Action::CreateChild, &authz::FLEET).await?;

        use nexus_db_schema::schema::rendezvous_debug_dataset::dsl;

        diesel::insert_into(dsl::rendezvous_debug_dataset)
            .values(dataset)
            .on_conflict(dsl::id)
            .do_nothing()
            .returning(RendezvousDebugDataset::as_select())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Mark a debug dataset as tombstoned
    ///
    /// If the dataset has already been tombstoned, does nothing.
    ///
    /// # Returns
    ///
    /// Returns `true` if the dataset was tombstoned; `false` if the dataset was
    /// already tombstoned or does not exist.
    pub async fn debug_dataset_tombstone(
        &self,
        opctx: &OpContext,
        dataset_id: DatasetUuid,
        blueprint_id: BlueprintUuid,
    ) -> Result<bool, Error> {
        opctx.authorize(authz::Action::Delete, &authz::FLEET).await?;

        use nexus_db_schema::schema::rendezvous_debug_dataset::dsl;

        diesel::update(dsl::rendezvous_debug_dataset)
            .filter(dsl::id.eq(to_db_typed_uuid(dataset_id)))
            .filter(dsl::time_tombstoned.is_null())
            .set((
                dsl::time_tombstoned.eq(Utc::now()),
                dsl::blueprint_id_when_tombstoned
                    .eq(to_db_typed_uuid(blueprint_id)),
            ))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map(|rows_modified| match rows_modified {
                0 => false,
                1 => true,
                n => unreachable!(
                    "update restricted to 1 row return {n} rows modified"
                ),
            })
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::pub_test_utils::TestDatabase;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::ZpoolUuid;

    #[tokio::test]
    async fn insert_if_not_exists() {
        let logctx = dev::test_setup_log("insert_if_not_exists");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // There should be no datasets initially.
        assert_eq!(
            datastore.debug_dataset_list_all_batched(opctx).await.unwrap(),
            []
        );

        // Inserting a new dataset should succeed.
        let dataset1 = datastore
            .debug_dataset_insert_if_not_exists(
                opctx,
                RendezvousDebugDataset::new(
                    DatasetUuid::new_v4(),
                    ZpoolUuid::new_v4(),
                    BlueprintUuid::new_v4(),
                ),
            )
            .await
            .expect("query succeeded")
            .expect("inserted dataset");
        let expected_datasets = vec![dataset1.clone()];
        assert_eq!(
            datastore.debug_dataset_list_all_batched(opctx).await.unwrap(),
            expected_datasets,
        );

        // Attempting to insert another dataset with the same ID should succeed
        // without updating the existing record. We'll check this by passing a
        // pool / blueprint ID but confirming we get back exactly the same
        // record as we did previously.
        let insert_again_result = datastore
            .debug_dataset_insert_if_not_exists(
                opctx,
                RendezvousDebugDataset::new(
                    dataset1.id(),
                    ZpoolUuid::new_v4(),
                    BlueprintUuid::new_v4(),
                ),
            )
            .await
            .expect("query succeeded");
        assert_eq!(insert_again_result, None);
        assert_eq!(
            datastore.debug_dataset_list_all_batched(opctx).await.unwrap(),
            expected_datasets,
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn tombstone() {
        let logctx = dev::test_setup_log("tombstone");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // There should be no datasets initially.
        assert_eq!(
            datastore.debug_dataset_list_all_batched(opctx).await.unwrap(),
            []
        );

        let dataset = RendezvousDebugDataset::new(
            DatasetUuid::new_v4(),
            ZpoolUuid::new_v4(),
            BlueprintUuid::new_v4(),
        );

        // Tombstoning a nonexistent dataset should do nothing.
        let tombstone_blueprint_id = BlueprintUuid::new_v4();
        let tombstoned = datastore
            .debug_dataset_tombstone(
                opctx,
                dataset.id(),
                tombstone_blueprint_id,
            )
            .await
            .expect("query succeeded");
        assert!(!tombstoned);

        // Insert the dataset and confirm we see it.
        let dataset = datastore
            .debug_dataset_insert_if_not_exists(opctx, dataset)
            .await
            .expect("query succeeded")
            .expect("inserted dataset");
        assert_eq!(
            datastore.debug_dataset_list_all_batched(opctx).await.unwrap(),
            std::slice::from_ref(&dataset),
        );

        // Tombstoning it should now succeed, and we should see the tombstoned
        // version when listing all datasets.
        let tombstoned = datastore
            .debug_dataset_tombstone(
                opctx,
                dataset.id(),
                tombstone_blueprint_id,
            )
            .await
            .expect("query succeeded");
        assert!(tombstoned);
        let all_datasets =
            datastore.debug_dataset_list_all_batched(opctx).await.unwrap();
        assert_eq!(all_datasets.len(), 1);
        assert_eq!(all_datasets[0].id(), dataset.id());
        assert_eq!(all_datasets[0].pool_id(), dataset.pool_id());
        assert_eq!(
            all_datasets[0].blueprint_id_when_created(),
            dataset.blueprint_id_when_created()
        );
        assert_eq!(
            all_datasets[0].blueprint_id_when_tombstoned(),
            Some(tombstone_blueprint_id)
        );
        assert!(all_datasets[0].is_tombstoned());

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
