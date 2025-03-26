// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods for webhook events and event delivery dispatching.

use super::DataStore;
use crate::context::OpContext;
use crate::db::error::ErrorHandler;
use crate::db::error::public_error_from_diesel;
use crate::db::model::WebhookEvent;
use crate::db::model::WebhookEventClass;
use crate::db::model::WebhookEventIdentity;
use crate::db::schema::webhook_event::dsl as event_dsl;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use diesel::result::OptionalExtension;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::UpdateResult;
use omicron_uuid_kinds::{GenericUuid, WebhookEventUuid};

impl DataStore {
    pub async fn webhook_event_create(
        &self,
        opctx: &OpContext,
        id: WebhookEventUuid,
        event_class: WebhookEventClass,
        event: serde_json::Value,
    ) -> CreateResult<WebhookEvent> {
        let conn = self.pool_connection_authorized(&opctx).await?;
        diesel::insert_into(event_dsl::webhook_event)
            .values(WebhookEvent {
                identity: WebhookEventIdentity::new(id),
                time_dispatched: None,
                event_class,
                event,
                num_dispatched: 0,
            })
            .returning(WebhookEvent::as_returning())
            .get_result_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn webhook_event_select_next_for_dispatch(
        &self,
        opctx: &OpContext,
    ) -> Result<Option<WebhookEvent>, Error> {
        let conn = self.pool_connection_authorized(&opctx).await?;
        event_dsl::webhook_event
            .filter(event_dsl::time_dispatched.is_null())
            .order_by(event_dsl::time_created.asc())
            .select(WebhookEvent::as_select())
            .first_async(&*conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn webhook_event_mark_dispatched(
        &self,
        opctx: &OpContext,
        event_id: &WebhookEventUuid,
        subscribed: usize,
    ) -> UpdateResult<usize> {
        let subscribed = i64::try_from(subscribed).map_err(|_| {
            // that is way too many webhook receivers!
            Error::internal_error(
                "webhook event subscribed count exceeds i64::MAX",
            )
        })?;
        let conn = self.pool_connection_authorized(&opctx).await?;
        diesel::update(event_dsl::webhook_event)
            .filter(event_dsl::id.eq(event_id.into_untyped_uuid()))
            .filter(
                // Update the event record if one of the following is true:
                // - The `time_dispatched`` field has not already been set, or
                // - `time_dispatched` IS set, but `num_dispatched` is less than
                //   the number of deliveries we believe has been dispatched.
                //   This may be the case if a webhook receiver which is
                //   subscribed to this event was added concurrently with
                //   another Nexus' dispatching the event, and we dispatched the
                //   event to that receiver but the other Nexus did not. In that
                //   case, we would like to update the record to indicate the
                //   correct number of subscribers.
                event_dsl::time_dispatched
                    .is_null()
                    .or(event_dsl::num_dispatched.le(subscribed)),
            )
            .set((
                event_dsl::time_dispatched.eq(diesel::dsl::now),
                event_dsl::num_dispatched.eq(subscribed),
            ))
            .execute_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }
}
