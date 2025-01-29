// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods for webhook events and event delivery dispatching.

use super::DataStore;
use crate::context::OpContext;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::collection_insert::DatastoreCollection;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::model::WebhookDelivery;
use crate::db::model::WebhookEvent;
use crate::db::model::WebhookEventClass;
use crate::db::model::WebhookReceiver;
use crate::db::pool::DbConnection;
use crate::db::schema::webhook_delivery::dsl as delivery_dsl;
use crate::db::schema::webhook_event::dsl as event_dsl;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use diesel::result::OptionalExtension;
use nexus_types::identity::Resource;
use nexus_types::internal_api::background::WebhookDispatched;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::{GenericUuid, WebhookEventUuid, WebhookReceiverUuid};

impl DataStore {
    pub async fn webhook_event_create(
        &self,
        opctx: &OpContext,
        id: WebhookEventUuid,
        event_class: WebhookEventClass,
        event: serde_json::Value,
    ) -> CreateResult<WebhookEvent> {
        let conn = self.pool_connection_authorized(&opctx).await?;
        let now =
            diesel::dsl::now.into_sql::<diesel::pg::sql_types::Timestamptz>();
        diesel::insert_into(event_dsl::webhook_event)
            .values((
                event_dsl::event_class.eq(event_class),
                event_dsl::id.eq(id.into_untyped_uuid()),
                event_dsl::time_created.eq(now),
                event_dsl::event.eq(event),
            ))
            .returning(WebhookEvent::as_returning())
            .get_result_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn webhook_event_dispatch_next(
        &self,
        opctx: &OpContext,
    ) -> Result<Option<WebhookDispatched>, Error> {
        let conn = self.pool_connection_authorized(&opctx).await?;
        self.transaction_retry_wrapper("webhook_event_dispatch_next")
            .transaction(&conn, |conn| async move {
                // Select the next webhook event in need of dispatching.
                //
                // This performs a `SELECT ... FOR UPDATE SKIP LOCKED` on the
                // `webhook_event` table, returning the oldest webhook event which has not
                // yet been dispatched to receivers and which is not actively being
                // dispatched in another transaction.
                // NOTE: it would be kinda nice if this query could also select the
                // webhook receivers subscribed to this event, but this requires
                // a `FOR UPDATE OF webhook_event` clause to indicate that we only wish
                // to lock the `webhook_event` row and not the receiver.
                // Unfortunately, I don't believe Diesel supports this at present.
                let Some(event) = event_dsl::webhook_event
                    .filter(event_dsl::time_dispatched.is_null())
                    .order_by(event_dsl::time_created.asc())
                    .limit(1)
                    .for_update()
                    // TODO(eliza): AGH SKIP LOCKED IS NOT IMPLEMENTED IN CRDB...
                    // .skip_locked()
                    .select(WebhookEvent::as_select())
                    .get_result_async(&conn)
                    .await
                    .optional()?
                else {
                    slog::debug!(
                        opctx.log,
                        "no unlocked webhook events in need of dispatching",
                    );
                    return Ok(None);
                };

                let mut result = WebhookDispatched {
                    event_id: event.id.into(),
                    dispatched: 0,
                    receivers_gone: 0,
                };

                // Find receivers subscribed to this event's class.
                let rxs = self
                    .webhook_rx_list_subscribed_to_event_on_conn(
                        &event.event_class,
                        &conn,
                    )
                    .await?;

                slog::debug!(
                    &opctx.log,
                    "found {} receivers subscribed to webhook event", rxs.len();
                    "event_id" => ?event.id,
                    "event_class" => %event.event_class,
                    "receivers" => ?rxs.len(),
                );

                // Create dispatch entries for each receiver subscribed to this
                // event class.
                for (rx, sub) in rxs {
                    let rx_id = rx.id();
                    slog::trace!(
                        &opctx.log,
                        "found receiver subscribed to event";
                        "event_id" => ?event.id,
                        "event_class" => %event.event_class,
                        "receiver" => ?rx.name(),
                        "receiver_id" => ?rx_id,
                        "glob" => ?sub.glob,
                    );
                    match self
                        .webhook_event_insert_delivery_on_conn(
                            &opctx.log, &event, &rx_id, &conn,
                        )
                        .await
                    {
                        Ok(_) => result.dispatched += 1,
                        Err(AsyncInsertError::CollectionNotFound) => {
                            // The receiver has been deleted while we were
                            // trying to dispatch an event to it. That's fine;
                            // rather than aborting the transaction and having
                            // to do all this stuff over, let's just keep going.
                            slog::debug!(
                                &opctx.log,
                                "cannot dispatch event to a receiver that has been deleted";
                                "event_id" => ?event.id,
                                "event_class" => %event.event_class,
                                "receiver" => ?rx.name(),
                                "receiver_id" => ?rx_id,
                            );
                            result.receivers_gone += 1;
                        },
                        Err(AsyncInsertError::DatabaseError(e)) => return Err(e),
                    }
                }

                // Finally, set the dispatched timestamp for the event so it
                // won't be dispatched by someone else.
                diesel::update(event_dsl::webhook_event)
                    .filter(event_dsl::id.eq(event.id))
                    .filter(event_dsl::time_dispatched.is_null())
                    .set(event_dsl::time_dispatched.eq(diesel::dsl::now))
                    .execute_async(&conn)
                    .await?;

                Ok(Some(result))
            })
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    async fn webhook_event_insert_delivery_on_conn(
        &self,
        log: &slog::Logger,
        event: &WebhookEvent,
        rx_id: &WebhookReceiverUuid,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Result<WebhookDelivery, AsyncInsertError> {
        loop {
            let delivery: Option<WebhookDelivery> =
                WebhookReceiver::insert_resource(
                    rx_id.into_untyped_uuid(),
                    diesel::insert_into(delivery_dsl::webhook_delivery)
                        .values(WebhookDelivery::new(&event, rx_id))
                        .on_conflict(delivery_dsl::id)
                        .do_nothing(),
                )
                .insert_and_get_optional_result_async(conn)
                .await?;
            match delivery {
                Some(delivery) => {
                    // XXX(eliza): is `Debug` too noisy for this?
                    slog::debug!(
                        log,
                        "dispatched webhook event to receiver";
                        "event_id" => ?event.id,
                        "event_class" => %event.event_class,
                        "receiver_id" => ?rx_id,
                    );
                    return Ok(delivery);
                }
                // The `ON CONFLICT (id) DO NOTHING` clause triggers if there's
                // already a delivery entry with this UUID --- indicating a UUID
                // collision. With 128 bits of random UUID, the chances of this
                // happening are incredibly unlikely, but let's handle it
                // gracefully nonetheless by trying again with a new UUID...
                None => {
                    slog::warn!(
                        &log,
                        "webhook delivery UUID collision, retrying...";
                        "event_id" => ?event.id,
                        "event_class" => %event.event_class,
                        "receiver_id" => ?rx_id,
                    );
                }
            }
        }
    }
}
