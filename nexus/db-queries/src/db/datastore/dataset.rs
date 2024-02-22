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

        let zpool_id = dataset.pool_id;
        Zpool::insert_resource(
            zpool_id,
            diesel::insert_into(dsl::dataset)
                .values(dataset.clone())
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
                    &dataset.id().to_string(),
                ),
            ),
        })
    }

    /// List one page of datasets
    ///
    /// If `filter_kind` is `Some(value)`, only datasets with a `kind` matching
    /// `value` will be returned. If `filter_kind` is `None`, all datasets will
    /// be returned.
    pub async fn dataset_list(
        &self,
        opctx: &OpContext,
        filter_kind: Option<DatasetKind>,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<Dataset> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        use db::schema::dataset::dsl;

        let mut query = paginated(dsl::dataset, dsl::id, pagparams);

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
}
