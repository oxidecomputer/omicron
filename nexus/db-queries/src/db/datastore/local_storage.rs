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
use crate::db::model::LocalStorageDataset;
use crate::db::model::Zpool;
use crate::db::pagination::Paginator;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_types::identity::Asset;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
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
    ) -> ListResultVec<LocalStorageDataset> {
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
                &|d: &nexus_db_model::LocalStorageDataset| {
                    d.id().into_untyped_uuid()
                },
            );
            all_datasets.extend(batch);
        }

        Ok(all_datasets)
    }

    /// List one page of local storage datasets
    async fn local_storage_dataset_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<LocalStorageDataset> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        use nexus_db_schema::schema::local_storage_dataset::dsl;

        paginated(dsl::local_storage_dataset, dsl::id, pagparams)
            .filter(dsl::time_deleted.is_null())
            .select(LocalStorageDataset::as_select())
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
        dataset: LocalStorageDataset,
    ) -> CreateResult<Option<LocalStorageDataset>> {
        use nexus_db_schema::schema::local_storage_dataset::dsl;

        let zpool_id = dataset.pool_id;
        Zpool::insert_resource(
            zpool_id,
            diesel::insert_into(dsl::local_storage_dataset)
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
}
