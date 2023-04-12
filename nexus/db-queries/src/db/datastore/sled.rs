// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Sled`]s.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::error::TransactionError;
use crate::db::identity::Asset;
use crate::db::model::Sled;
use crate::db::model::SledResource;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use omicron_common::api::external;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::ResourceType;
use uuid::Uuid;

impl DataStore {
    /// Stores a new sled in the database.
    pub async fn sled_upsert(&self, sled: Sled) -> CreateResult<Sled> {
        use db::schema::sled::dsl;
        diesel::insert_into(dsl::sled)
            .values(sled.clone())
            .on_conflict(dsl::id)
            .do_update()
            .set((
                dsl::time_modified.eq(Utc::now()),
                dsl::ip.eq(sled.ip),
                dsl::port.eq(sled.port),
                dsl::rack_id.eq(sled.rack_id),
                dsl::is_scrimlet.eq(sled.is_scrimlet()),
            ))
            .returning(Sled::as_returning())
            .get_result_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::Sled,
                        &sled.id().to_string(),
                    ),
                )
            })
    }

    pub async fn sled_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<Sled> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        use db::schema::sled::dsl;
        paginated(dsl::sled, dsl::id, pagparams)
            .select(Sled::as_select())
            .load_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn sled_reservation_create(
        &self,
        opctx: &OpContext,
        resource_id: Uuid,
        resource_kind: db::model::SledResourceKind,
        resources: db::model::Resources,
    ) -> CreateResult<db::model::SledResource> {
        #[derive(Debug)]
        enum SledReservationError {
            NotFound,
        }
        type TxnError = TransactionError<SledReservationError>;

        self.pool_authorized(opctx)
            .await?
            .transaction_async(|conn| async move {
                use db::schema::sled_resource::dsl as resource_dsl;
                // Check if resource ID already exists - if so, return it.
                let old_resource = resource_dsl::sled_resource
                    .filter(resource_dsl::id.eq(resource_id))
                    .select(SledResource::as_select())
                    .limit(1)
                    .load_async(&conn)
                    .await?;

                if !old_resource.is_empty() {
                    return Ok(old_resource[0].clone());
                }

                // If it doesn't already exist, find a sled with enough space
                // for the resources we're requesting.
                use db::schema::sled::dsl as sled_dsl;
                // This answers the boolean question:
                // "Does the SUM of all hardware thread usage, plus the one we're trying
                // to allocate, consume less threads than exists on the sled?"
                let sled_has_space_for_threads =
                    (diesel::dsl::sql::<diesel::sql_types::BigInt>(&format!(
                        "COALESCE(SUM(CAST({} as INT8)), 0)",
                        resource_dsl::hardware_threads::NAME
                    )) + resources.hardware_threads)
                        .le(sled_dsl::usable_hardware_threads);
                // This answers the boolean question:
                // "Does the SUM of all RAM usage, plus the one we're trying
                // to allocate, consume less RAM than exists on the sled?"
                let sled_has_space_for_rss =
                    (diesel::dsl::sql::<diesel::sql_types::BigInt>(&format!(
                        "COALESCE(SUM(CAST({} as INT8)), 0)",
                        resource_dsl::rss_ram::NAME
                    )) + resources.rss_ram)
                        .le(sled_dsl::usable_physical_ram);
                sql_function!(fn random() -> diesel::sql_types::Float);
                let sled_targets = sled_dsl::sled
                    // LEFT JOIN so we can observe sleds with no
                    // currently-allocated resources as potential targets
                    .left_join(
                        resource_dsl::sled_resource
                            .on(resource_dsl::sled_id.eq(sled_dsl::id)),
                    )
                    .group_by(sled_dsl::id)
                    .having(
                        sled_has_space_for_threads.and(sled_has_space_for_rss),
                        // TODO: We should also validate the reservoir space, when it exists.
                    )
                    .filter(sled_dsl::time_deleted.is_null())
                    .select(sled_dsl::id)
                    .order(random())
                    .limit(1)
                    .get_results_async::<Uuid>(&conn)
                    .await?;

                if sled_targets.is_empty() {
                    return Err(TxnError::CustomError(
                        SledReservationError::NotFound,
                    ));
                }

                // Create a SledResource record, associate it with the target
                // sled.
                let resource = SledResource::new(
                    resource_id,
                    sled_targets[0],
                    resource_kind,
                    resources,
                );

                Ok(diesel::insert_into(resource_dsl::sled_resource)
                    .values(resource)
                    .returning(SledResource::as_returning())
                    .get_result_async(&conn)
                    .await?)
            })
            .await
            .map_err(|e| match e {
                TxnError::CustomError(SledReservationError::NotFound) => {
                    external::Error::unavail(
                        "No sleds can fit the requested instance",
                    )
                }
                TxnError::Pool(e) => {
                    public_error_from_diesel_pool(e, ErrorHandler::Server)
                }
            })
    }

    pub async fn sled_reservation_delete(
        &self,
        opctx: &OpContext,
        resource_id: Uuid,
    ) -> DeleteResult {
        use db::schema::sled_resource::dsl as resource_dsl;
        diesel::delete(resource_dsl::sled_resource)
            .filter(resource_dsl::id.eq(resource_id))
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(e, ErrorHandler::Server)
            })?;
        Ok(())
    }
}
