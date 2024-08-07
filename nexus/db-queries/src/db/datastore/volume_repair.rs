// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`VolumeRepair`]s.

use super::DataStore;
use crate::db;
use crate::db::datastore::OpContext;
use crate::db::datastore::RunnableQuery;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::model::VolumeRepair;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use diesel::result::Error as DieselError;
use omicron_common::api::external::Error;
use uuid::Uuid;

impl DataStore {
    pub(super) fn volume_repair_insert_query(
        volume_id: Uuid,
        repair_id: Uuid,
    ) -> impl RunnableQuery<VolumeRepair> {
        use db::schema::volume_repair::dsl;

        diesel::insert_into(dsl::volume_repair)
            .values(VolumeRepair { volume_id, repair_id })
    }

    pub async fn volume_repair_lock(
        &self,
        opctx: &OpContext,
        volume_id: Uuid,
        repair_id: Uuid,
    ) -> Result<(), Error> {
        let conn = self.pool_connection_authorized(opctx).await?;
        Self::volume_repair_insert_query(volume_id, repair_id)
            .execute_async(&*conn)
            .await
            .map(|_| ())
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub(super) fn volume_repair_delete_query(
        volume_id: Uuid,
        repair_id: Uuid,
    ) -> impl RunnableQuery<VolumeRepair> {
        use db::schema::volume_repair::dsl;

        diesel::delete(
            dsl::volume_repair
                .filter(dsl::volume_id.eq(volume_id))
                .filter(dsl::repair_id.eq(repair_id)),
        )
    }

    pub async fn volume_repair_unlock(
        &self,
        opctx: &OpContext,
        volume_id: Uuid,
        repair_id: Uuid,
    ) -> Result<(), Error> {
        let conn = self.pool_connection_authorized(opctx).await?;
        Self::volume_repair_delete_query(volume_id, repair_id)
            .execute_async(&*conn)
            .await
            .map(|_| ())
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn volume_repair_get(
        conn: &async_bb8_diesel::Connection<db::DbConnection>,
        volume_id: Uuid,
        repair_id: Uuid,
    ) -> Result<VolumeRepair, DieselError> {
        use db::schema::volume_repair::dsl;

        dsl::volume_repair
            .filter(dsl::repair_id.eq(repair_id))
            .filter(dsl::volume_id.eq(volume_id))
            .first_async::<VolumeRepair>(conn)
            .await
    }
}
