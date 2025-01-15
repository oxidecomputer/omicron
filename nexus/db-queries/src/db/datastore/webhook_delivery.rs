// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods for webhook event deliveries

use super::DataStore;
use crate::context::OpContext;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::model::WebhookDelivery;
use crate::db::model::WebhookDeliveryAttempt;
use crate::db::model::WebhookEvent;
use crate::db::schema::webhook_delivery::dsl;
use crate::db::schema::webhook_event::dsl as event_dsl;
use crate::db::update_and_check::UpdateAndCheck;
use crate::db::update_and_check::UpdateStatus;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::TimeDelta;
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_uuid_kinds::{GenericUuid, OmicronZoneUuid, WebhookReceiverUuid};

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DeliveryAttemptState {
    Started,
    AlreadyCompleted(DateTime<Utc>),
    InProgress { nexus_id: OmicronZoneUuid, started: DateTime<Utc> },
}

impl DataStore {
    pub async fn webhook_rx_delivery_list_ready(
        &self,
        opctx: &OpContext,
        rx_id: &WebhookReceiverUuid,
        lease_timeout: TimeDelta,
    ) -> ListResultVec<(WebhookDelivery, WebhookEvent)> {
        let conn = self.pool_connection_authorized(opctx).await?;
        let now =
            diesel::dsl::now.into_sql::<diesel::pg::sql_types::Timestamptz>();
        dsl::webhook_delivery
            .filter(dsl::time_completed.is_null())
            .filter(dsl::rx_id.eq(rx_id.into_untyped_uuid()))
            .filter(
                (dsl::time_delivery_started
                    .is_null()
                    .and(dsl::deliverator_id.is_null()))
                .or(dsl::time_delivery_started.is_not_null().and(
                    dsl::time_delivery_started
                        .le(now.nullable() - lease_timeout),
                )),
            )
            .order_by(dsl::time_created.asc())
            // Join with the `webhook_event` table to get the event class, which
            // is necessary to construct delivery requests.
            .inner_join(
                event_dsl::webhook_event.on(event_dsl::id.eq(dsl::event_id)),
            )
            .select((WebhookDelivery::as_select(), WebhookEvent::as_select()))
            .load_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn webhook_delivery_start_attempt(
        &self,
        opctx: &OpContext,
        delivery: &WebhookDelivery,
        nexus_id: &OmicronZoneUuid,
        lease_timeout: TimeDelta,
    ) -> Result<DeliveryAttemptState, Error> {
        let conn = self.pool_connection_authorized(opctx).await?;
        let now =
            diesel::dsl::now.into_sql::<diesel::pg::sql_types::Timestamptz>();
        let id = delivery.id.into_untyped_uuid();
        let updated = diesel::update(dsl::webhook_delivery)
            .filter(dsl::time_completed.is_null())
            .filter(dsl::id.eq(id))
            .filter(
                (dsl::time_delivery_started
                    .is_null()
                    .and(dsl::deliverator_id.is_null()))
                .or(dsl::time_delivery_started.is_not_null().and(
                    dsl::time_delivery_started
                        .le(now.nullable() - lease_timeout),
                )),
            )
            .set((
                dsl::time_delivery_started.eq(now.nullable()),
                dsl::deliverator_id.eq(nexus_id.into_untyped_uuid()),
            ))
            .check_if_exists::<WebhookDelivery>(id)
            .execute_and_check(&conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        match updated.status {
            UpdateStatus::Updated => Ok(DeliveryAttemptState::Started),
            UpdateStatus::NotUpdatedButExists => {
                if let Some(completed) = updated.found.time_completed {
                    return Ok(DeliveryAttemptState::AlreadyCompleted(
                        completed,
                    ));
                }

                if let Some(started) = updated.found.time_delivery_started {
                    let nexus_id =
                        updated.found.deliverator_id.ok_or_else(|| {
                            Error::internal_error(
                                "if a delivery attempt has a last started \
                                 timestamp, the database should ensure that \
                                 it also has a Nexus ID",
                            )
                        })?;
                    return Ok(DeliveryAttemptState::InProgress {
                        nexus_id: nexus_id.into(),
                        started,
                    });
                }

                Err(Error::internal_error("couldn't start delivery attempt for some secret third reason???"))
            }
        }
    }

    pub async fn webhook_delivery_finish_attempt(
        &self,
        opctx: &OpContext,
        delivery: &WebhookDelivery,
        nexus_id: &OmicronZoneUuid,
        result: &WebhookDeliveryAttempt,
    ) -> Result<(), Error> {
        Err(Error::internal_error("TODO ELIZA DO THIS PART"))
    }
}
