// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] helpers for working with records related to local storage.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::collection_insert::DatastoreCollection;
use crate::db::datastore::SQL_BATCH_SIZE;
use crate::db::model::RendezvousLocalStorageDataset;
use crate::db::model::Zpool;
use crate::db::model::to_db_typed_uuid;
use crate::db::pagination::Paginator;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_uuid_kinds::BlueprintUuid;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::GenericUuid;
use uuid::Uuid;

impl DataStore {
    /// List all LocalStorage datasets, making as many queries as needed to get
    /// them all
    ///
    /// This should generally not be used in API handlers or other
    /// latency-sensitive contexts, but it can make sense in saga actions or
    /// background tasks.
    pub async fn local_storage_dataset_list_all_batched(
        &self,
        opctx: &OpContext,
    ) -> ListResultVec<RendezvousLocalStorageDataset> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        opctx.check_complex_operations_allowed()?;

        let mut all_datasets = Vec::new();
        let mut paginator = Paginator::new(
            SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        while let Some(p) = paginator.next() {
            let batch = self
                .local_storage_dataset_list(opctx, &p.current_pagparams())
                .await?;
            paginator = p.found_batch(
                &batch,
                &|d: &nexus_db_model::RendezvousLocalStorageDataset| {
                    d.id().into_untyped_uuid()
                },
            );
            all_datasets.extend(batch);
        }

        Ok(all_datasets)
    }

    /// List one page of local storage datasets
    ///
    /// This fetches all debug datasets, including those that have been
    /// tombstoned.
    async fn local_storage_dataset_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<RendezvousLocalStorageDataset> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        use nexus_db_schema::schema::rendezvous_local_storage_dataset::dsl;

        paginated(dsl::rendezvous_local_storage_dataset, dsl::id, pagparams)
            .select(RendezvousLocalStorageDataset::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Stores a new dataset in the database, but only if a dataset with the
    /// given `id` does not already exist
    ///
    /// Does not update existing rows. If a dataset with the given ID already
    /// exists, returns `Ok(None)`.
    pub async fn local_storage_dataset_insert_if_not_exists(
        &self,
        opctx: &OpContext,
        dataset: RendezvousLocalStorageDataset,
    ) -> CreateResult<Option<RendezvousLocalStorageDataset>> {
        opctx.authorize(authz::Action::CreateChild, &authz::FLEET).await?;

        use nexus_db_schema::schema::rendezvous_local_storage_dataset::dsl;

        let zpool_id = dataset.pool_id;
        Zpool::insert_resource(
            zpool_id,
            diesel::insert_into(dsl::rendezvous_local_storage_dataset)
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
                lookup_type: LookupType::by_id(zpool_id),
            },
            AsyncInsertError::DatabaseError(e) => {
                public_error_from_diesel(e, ErrorHandler::Server)
            }
        })
    }

    /// Tombstone a local storage dataset, if it isn't already
    pub async fn local_storage_dataset_tombstone(
        &self,
        opctx: &OpContext,
        dataset_id: DatasetUuid,
        blueprint_id_when_tombstoned: BlueprintUuid,
    ) -> Result<bool, Error> {
        opctx.authorize(authz::Action::Delete, &authz::FLEET).await?;

        use nexus_db_schema::schema::rendezvous_local_storage_dataset::dsl;

        diesel::update(dsl::rendezvous_local_storage_dataset)
            .filter(dsl::id.eq(to_db_typed_uuid(dataset_id)))
            .filter(dsl::time_tombstoned.is_null())
            .set((
                dsl::time_tombstoned.eq(Utc::now()),
                dsl::blueprint_id_when_tombstoned
                    .eq(to_db_typed_uuid(blueprint_id_when_tombstoned)),
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
