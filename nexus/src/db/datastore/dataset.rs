// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Dataset`]s.

use super::DataStore;
use super::RunnableQuery;
use super::REGION_REDUNDANCY_THRESHOLD;
use crate::db;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::collection_insert::DatastoreCollection;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::identity::Asset;
use crate::db::model::Dataset;
use crate::db::model::DatasetKind;
use crate::db::model::Zpool;
use chrono::Utc;
use diesel::prelude::*;
use diesel::upsert::excluded;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;

impl DataStore {
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
        .insert_and_get_result_async(self.pool())
        .await
        .map_err(|e| match e {
            AsyncInsertError::CollectionNotFound => Error::ObjectNotFound {
                type_name: ResourceType::Zpool,
                lookup_type: LookupType::ById(zpool_id),
            },
            AsyncInsertError::DatabaseError(e) => {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::Dataset,
                        &dataset.id().to_string(),
                    ),
                )
            }
        })
    }

    pub(super) fn get_allocatable_datasets_query() -> impl RunnableQuery<Dataset>
    {
        use db::schema::dataset::dsl;

        dsl::dataset
            // We look for valid datasets (non-deleted crucible datasets).
            .filter(dsl::size_used.is_not_null())
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::kind.eq(DatasetKind::Crucible))
            .order(dsl::size_used.asc())
            // TODO: We admittedly don't actually *fail* any request for
            // running out of space - we try to send the request down to
            // crucible agents, and expect them to fail on our behalf in
            // out-of-storage conditions. This should undoubtedly be
            // handled more explicitly.
            .select(Dataset::as_select())
            .limit(REGION_REDUNDANCY_THRESHOLD.try_into().unwrap())
    }
}
