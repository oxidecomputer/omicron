// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Zpool`]s.

use super::DataStore;
use crate::authz;
use crate::db;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::collection_insert::DatastoreCollection;
use crate::db::datastore::OpContext;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::identity::Asset;
use crate::db::model::Sled;
use crate::db::model::Zpool;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use diesel::upsert::excluded;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use uuid::Uuid;
use nexus_db_model::PhysicalDiskKind;

impl DataStore {
    /// Stores a new zpool in the database.
    pub async fn zpool_upsert(&self, zpool: Zpool) -> CreateResult<Zpool> {
        use db::schema::zpool::dsl;

        let sled_id = zpool.sled_id;
        Sled::insert_resource(
            sled_id,
            diesel::insert_into(dsl::zpool)
                .values(zpool.clone())
                .on_conflict(dsl::id)
                .do_update()
                .set((
                    dsl::time_modified.eq(Utc::now()),
                    dsl::sled_id.eq(excluded(dsl::sled_id)),
                    dsl::total_size.eq(excluded(dsl::total_size)),
                )),
        )
        .insert_and_get_result_async(
            &*self.pool_connection_unauthorized().await?,
        )
        .await
        .map_err(|e| match e {
            AsyncInsertError::CollectionNotFound => Error::ObjectNotFound {
                type_name: ResourceType::Sled,
                lookup_type: LookupType::ById(sled_id),
            },
            AsyncInsertError::DatabaseError(e) => public_error_from_diesel(
                e,
                ErrorHandler::Conflict(
                    ResourceType::Zpool,
                    &zpool.id().to_string(),
                ),
            ),
        })
    }

    // XXX-dap
    //     /// Lists zpools in the system
    //     pub async fn zpool_list(
    //         &self,
    //         opctx: &OpContext,
    //         pagparams: &DataPageParams<'_, Uuid>,
    //     ) -> ListResultVec<Zpool> {
    //         opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
    //         use db::schema::zpool::dsl;
    //         paginated(dsl::zpool, dsl::id, pagparams)
    //             .select(Zpool::as_select())
    //             .load_async(&*self.pool_connection_authorized(opctx).await?)
    //             .await
    //             .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    //     }

    /// Paginates through all zpools on U.2 disks in sleds
    // XXX-dap maybe belongs in a "deployment" module instead
    pub async fn zpool_list_all_external(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<Zpool> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        // // XXX-dap saving the below in case I want to go back to it but it's a
        // // little denormalized so we'll do this in two queries instead, which is
        // // also simpler.
        // // To find the zpools on U.2 devices in sleds, we need to join the
        // // "sled", "zpool", and "physical_disk" tables.  The SQL looks like
        // // this:
        // //
        // // SELECT
        // //     sled.id, sled.ip, zpool.id
        // // FROM
        // //     sled,
        // //     zpool,
        // //     physical_disk
        // // WHERE
        // //      sled.id = zpool.sled_id
        // //  AND zpool.physical_disk_id = physical_disk.id
        // //  AND physical_disk.variant = 'u2'
        // //
        // // ORDER BY zpool.id

        use db::schema::physical_disk::dsl as dsl_physical_disk;
        use db::schema::zpool::dsl as dsl_zpool;
        paginated(dsl_zpool::zpool, dsl_zpool::id, pagparams)
            .inner_join(
                db::schema::physical_disk::table.on(
                    dsl_zpool::physical_disk_id.eq(dsl_physical_disk::id).and(
                        dsl_physical_disk::variant.eq(PhysicalDiskKind::U2),
                    ),
                ),
            )
            .select(Zpool::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }
}
