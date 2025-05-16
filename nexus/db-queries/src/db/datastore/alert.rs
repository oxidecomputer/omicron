// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods for alerts and alert delivery dispatching.

use super::DataStore;
use crate::context::OpContext;
use crate::db::model::Alert;
use crate::db::model::AlertClass;
use crate::db::model::AlertIdentity;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use diesel::result::OptionalExtension;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_schema::schema::alert::dsl as alert_dsl;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::UpdateResult;
use omicron_uuid_kinds::{AlertUuid, GenericUuid};

impl DataStore {
    pub async fn alert_create(
        &self,
        opctx: &OpContext,
        id: AlertUuid,
        class: AlertClass,
        payload: serde_json::Value,
    ) -> CreateResult<Alert> {
        let conn = self.pool_connection_authorized(&opctx).await?;
        diesel::insert_into(alert_dsl::alert)
            .values(Alert {
                identity: AlertIdentity::new(id),
                time_dispatched: None,
                class,
                payload,
                num_dispatched: 0,
            })
            .returning(Alert::as_returning())
            .get_result_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn alert_select_next_for_dispatch(
        &self,
        opctx: &OpContext,
    ) -> Result<Option<Alert>, Error> {
        let conn = self.pool_connection_authorized(&opctx).await?;
        alert_dsl::alert
            .filter(alert_dsl::time_dispatched.is_null())
            .order_by(alert_dsl::time_created.asc())
            .select(Alert::as_select())
            .first_async(&*conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn alert_mark_dispatched(
        &self,
        opctx: &OpContext,
        alert_id: &AlertUuid,
        subscribed: usize,
    ) -> UpdateResult<usize> {
        let subscribed = i64::try_from(subscribed).map_err(|_| {
            // that is way too many alert receivers!
            Error::internal_error("alert subscribed count exceeds i64::MAX")
        })?;
        let conn = self.pool_connection_authorized(&opctx).await?;
        diesel::update(alert_dsl::alert)
            .filter(alert_dsl::id.eq(alert_id.into_untyped_uuid()))
            .filter(
                // Update the alert record if one of the following is true:
                // - The `time_dispatched`` field has not already been set, or
                // - `time_dispatched` IS set, but `num_dispatched` is less than
                //   the number of deliveries we believe has been dispatched.
                //   This may be the case if a webhook receiver which is
                //   subscribed to this alert was added concurrently with
                //   another Nexus' dispatching the alert, and we dispatched the
                //   alert to that receiver but the other Nexus did not. In that
                //   case, we would like to update the record to indicate the
                //   correct number of subscribers.
                alert_dsl::time_dispatched
                    .is_null()
                    .or(alert_dsl::num_dispatched.le(subscribed)),
            )
            .set((
                alert_dsl::time_dispatched.eq(diesel::dsl::now),
                alert_dsl::num_dispatched.eq(subscribed),
            ))
            .execute_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }
}
