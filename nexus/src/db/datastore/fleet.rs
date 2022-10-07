// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Fleet`]s.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::identity::Asset;
use crate::db::model::Fleet;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use diesel::upsert::excluded;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::ResourceType;
use uuid::Uuid;

impl DataStore {
    pub async fn fleet_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<Fleet> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        use db::schema::fleet::dsl;
        paginated(dsl::fleet, dsl::id, pagparams)
            .select(Fleet::as_select())
            .load_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    /// Stores a new fleet in the database.
    ///
    /// This function is a no-op if the fleet already exists.
    pub async fn fleet_insert(
        &self,
        opctx: &OpContext,
        fleet: &Fleet,
    ) -> Result<Fleet, Error> {
        use db::schema::fleet::dsl;

        diesel::insert_into(dsl::fleet)
            .values(fleet.clone())
            .on_conflict(dsl::id)
            .do_update()
            // This is a no-op, since we conflicted on the ID.
            .set(dsl::id.eq(excluded(dsl::id)))
            .returning(Fleet::as_returning())
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::Fleet,
                        &fleet.id().to_string(),
                    ),
                )
            })
    }
}
