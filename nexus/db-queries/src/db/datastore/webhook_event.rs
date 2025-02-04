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
                let Some(event) = event_dsl::webhook_event
                    .filter(event_dsl::time_dispatched.is_null())
                    .order_by(event_dsl::time_created.asc())
                    .limit(1)
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
                        event.event_class,
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
                            &event, &rx_id, false, &conn
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
        event: &WebhookEvent,
        rx_id: &WebhookReceiverUuid,
        is_redelivery: bool,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Result<WebhookDelivery, AsyncInsertError> {
        let delivery: WebhookDelivery = WebhookReceiver::insert_resource(
            rx_id.into_untyped_uuid(),
            diesel::insert_into(delivery_dsl::webhook_delivery)
                .values(WebhookDelivery::new(&event, rx_id, is_redelivery)),
        )
        .insert_and_get_result_async(conn)
        .await?;
        Ok(delivery)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::db::pub_test_utils::TestDatabase;
    use omicron_test_utils::dev;

    #[tokio::test]
    async fn test_dispatched_deliveries_are_unique_per_rx() {
        // Test setup
        let logctx =
            dev::test_setup_log("test_dispatched_deliveries_are_unique_per_rx");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let conn = db.pool_connection_for_tests().await;

        let rx_id = WebhookReceiverUuid::new_v4();
        let event_id = WebhookEventUuid::new_v4();
        let event = datastore
            .webhook_event_create(
                &opctx,
                event_id,
                WebhookEventClass::TestFoo,
                serde_json::json!({
                    "answer": 42,
                }),
            )
            .await
            .expect("can't create ye event");

        let delivery1 = datastore
            .webhook_event_insert_delivery_on_conn(&event, &rx_id, false, conn)
            .await
            .expect("delivery 1 should insert");

        let delivery2 = datastore
            .webhook_event_insert_delivery_on_conn(&event, &rx_id, false, conn)
            .await;
        dbg!(delivery2).expect_err("unique constraint should be violated");

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
