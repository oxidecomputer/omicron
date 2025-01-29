// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods for webhook event deliveries

use super::DataStore;
use crate::context::OpContext;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::model::SqlU8;
use crate::db::model::WebhookDelivery;
use crate::db::model::WebhookDeliveryAttempt;
use crate::db::model::WebhookEventClass;
use crate::db::schema::webhook_delivery::dsl;
use crate::db::schema::webhook_delivery_attempt::dsl as attempt_dsl;
use crate::db::schema::webhook_event::dsl as event_dsl;
use crate::db::update_and_check::UpdateAndCheck;
use crate::db::update_and_check::UpdateAndQueryResult;
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
    ) -> ListResultVec<(WebhookDelivery, WebhookEventClass)> {
        let conn = self.pool_connection_authorized(opctx).await?;
        let now =
            diesel::dsl::now.into_sql::<diesel::pg::sql_types::Timestamptz>();
        dsl::webhook_delivery
            .filter(dsl::time_completed.is_null())
            .filter(dsl::rx_id.eq(rx_id.into_untyped_uuid()))
            .filter(
                (dsl::deliverator_id.is_null()).or(dsl::time_delivery_started
                    .is_not_null()
                    .and(
                        dsl::time_delivery_started
                            .le(now.nullable() - lease_timeout),
                    )),
                // TODO(eliza): retry backoffs...?
            )
            .order_by(dsl::time_created.asc())
            // Join with the `webhook_event` table to get the event class, which
            // is necessary to construct delivery requests.
            .inner_join(
                event_dsl::webhook_event.on(event_dsl::id.eq(dsl::event_id)),
            )
            .select((WebhookDelivery::as_select(), event_dsl::event_class))
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
                dsl::deliverator_id.is_null().or(dsl::time_delivery_started
                    .is_not_null()
                    .and(
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
        attempt: &WebhookDeliveryAttempt,
    ) -> Result<(), Error> {
        const MAX_ATTEMPTS: u8 = 4;
        let conn = self.pool_connection_authorized(opctx).await?;
        diesel::insert_into(attempt_dsl::webhook_delivery_attempt)
            .values(attempt.clone())
            .on_conflict((attempt_dsl::delivery_id, attempt_dsl::attempt))
            .do_nothing()
            .returning(WebhookDeliveryAttempt::as_returning())
            .execute_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        // Has the delivery either completed successfully or exhausted all of
        // its retry attempts?
        let succeeded =
            attempt.result == nexus_db_model::WebhookDeliveryResult::Succeeded;
        let failed_permanently = *attempt.attempt >= MAX_ATTEMPTS;
        let (completed, new_nexus_id) = if succeeded || failed_permanently {
            // If the delivery has succeeded or failed permanently, set the
            // "time_completed" timestamp to mark it as finished. Also, leave
            // the  delivering Nexus ID in place to maintain a record of who
            // finished the delivery.
            (Some(Utc::now()), Some(nexus_id.into_untyped_uuid()))
        } else {
            // Otherwise, "unlock" the delivery for other nexii.
            (None, None)
        };
        let prev_attempts = SqlU8::new((*attempt.attempt) - 1);
        let UpdateAndQueryResult { status, found } =
            diesel::update(dsl::webhook_delivery)
                .filter(dsl::id.eq(delivery.id.into_untyped_uuid()))
                .filter(dsl::deliverator_id.eq(nexus_id.into_untyped_uuid()))
                .filter(dsl::attempts.eq(prev_attempts))
                // Don't mark a delivery as completed if it's already completed!
                .filter(dsl::time_completed.is_null())
                .set((
                    dsl::time_completed.eq(completed),
                    // XXX(eliza): hmm this might be racy; we should probably increment this
                    // in place and use it to determine the attempt number?
                    dsl::attempts.eq(attempt.attempt),
                    dsl::deliverator_id.eq(new_nexus_id),
                ))
                .check_if_exists::<WebhookDelivery>(delivery.id)
                .execute_and_check(&conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;

        if status == UpdateStatus::Updated {
            return Ok(());
        }

        if let Some(other_nexus_id) = found.deliverator_id {
            return Err(Error::conflict(format!(
                "cannot mark delivery completed, as {other_nexus_id:?} was \
                 attempting to deliver it",
            )));
        }

        if found.time_completed.is_some() {
            return Err(Error::conflict(
                "delivery was already marked as completed",
            ));
        }

        if found.attempts != prev_attempts {
            return Err(Error::conflict("wrong number of delivery attempts"));
        }

        Err(Error::internal_error(
            "couldn't update delivery for some other reason i didn't think of here..."
        ))
    }
}
