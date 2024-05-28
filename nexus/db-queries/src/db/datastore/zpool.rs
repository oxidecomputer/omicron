// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Zpool`]s.

use super::DataStore;
use super::SQL_BATCH_SIZE;
use crate::authz;
use crate::db;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::collection_insert::DatastoreCollection;
use crate::db::datastore::OpContext;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::identity::Asset;
use crate::db::model::PhysicalDisk;
use crate::db::model::Sled;
use crate::db::model::Zpool;
use crate::db::pagination::paginated;
use crate::db::pagination::Paginator;
use crate::db::TransactionError;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use diesel::upsert::excluded;
use nexus_db_model::PhysicalDiskKind;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use uuid::Uuid;

impl DataStore {
    pub async fn zpool_insert(
        &self,
        opctx: &OpContext,
        zpool: Zpool,
    ) -> CreateResult<Zpool> {
        let conn = &*self.pool_connection_authorized(&opctx).await?;
        let zpool =
            Self::zpool_insert_on_connection(&conn, opctx, zpool).await?;
        Ok(zpool)
    }

    /// Stores a new zpool in the database.
    pub async fn zpool_insert_on_connection(
        conn: &async_bb8_diesel::Connection<db::DbConnection>,
        opctx: &OpContext,
        zpool: Zpool,
    ) -> Result<Zpool, TransactionError<Error>> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        use db::schema::zpool::dsl;

        let sled_id = zpool.sled_id;
        let pool = Sled::insert_resource(
            sled_id,
            diesel::insert_into(dsl::zpool)
                .values(zpool.clone())
                .on_conflict(dsl::id)
                .do_update()
                .set((
                    dsl::time_modified.eq(Utc::now()),
                    dsl::sled_id.eq(excluded(dsl::sled_id)),
                )),
        )
        .insert_and_get_result_async(conn)
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
        })?;

        Ok(pool)
    }

    /// Fetches a page of the list of all zpools on U.2 disks in all sleds
    async fn zpool_list_all_external(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<(Zpool, PhysicalDisk)> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        use db::schema::physical_disk::dsl as dsl_physical_disk;
        use db::schema::zpool::dsl as dsl_zpool;
        paginated(dsl_zpool::zpool, dsl_zpool::id, pagparams)
            .filter(dsl_zpool::time_deleted.is_null())
            .inner_join(
                db::schema::physical_disk::table.on(
                    dsl_zpool::physical_disk_id.eq(dsl_physical_disk::id).and(
                        dsl_physical_disk::variant.eq(PhysicalDiskKind::U2),
                    ),
                ),
            )
            .select((Zpool::as_select(), PhysicalDisk::as_select()))
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// List all zpools on U.2 disks in all sleds, making as many queries as
    /// needed to get them all
    ///
    /// This should generally not be used in API handlers or other
    /// latency-sensitive contexts, but it can make sense in saga actions or
    /// background tasks.
    pub async fn zpool_list_all_external_batched(
        &self,
        opctx: &OpContext,
    ) -> ListResultVec<(Zpool, PhysicalDisk)> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        opctx.check_complex_operations_allowed()?;
        let mut zpools = Vec::new();
        let mut paginator = Paginator::new(SQL_BATCH_SIZE);
        while let Some(p) = paginator.next() {
            let batch = self
                .zpool_list_all_external(opctx, &p.current_pagparams())
                .await?;
            paginator = p.found_batch(&batch, &|(z, _)| z.id());
            zpools.extend(batch);
        }

        Ok(zpools)
    }
}
