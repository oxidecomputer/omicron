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
use crate::db::model::WebhookDeliveryTrigger;
use crate::db::model::WebhookEvent;
use crate::db::model::WebhookEventClass;
use crate::db::model::WebhookReceiver;
use crate::db::pool::DbConnection;
use crate::db::schema::webhook_delivery::dsl as delivery_dsl;
use crate::db::schema::webhook_event::dsl as event_dsl;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use diesel::result::DatabaseErrorKind;
use diesel::result::Error as DieselError;
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
                            &event, &rx_id, WebhookDeliveryTrigger::Dispatch, &conn
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
        trigger: WebhookDeliveryTrigger,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Result<Option<WebhookDelivery>, AsyncInsertError> {
        let result: Result<WebhookDelivery, _> =
            WebhookReceiver::insert_resource(
                rx_id.into_untyped_uuid(),
                diesel::insert_into(delivery_dsl::webhook_delivery)
                    .values(WebhookDelivery::new(&event, rx_id, trigger)),
            )
            .insert_and_get_result_async(conn)
            .await;
        match result {
            Ok(delivery) => Ok(Some(delivery)),

            // If the UNIQUE constraint on the index
            // "one_webhook_event_dispatch_per_rx" is violated, then that just
            // means that a delivery has already been automatically dispatched
            // to this receiver. That's fine --- the goal is to ensure that one
            // exists, and if it already does, we don't need to do anything.
            //
            // Note this applies only to dispatched deliveries, not to
            // explicitly resent deliveries, as multiple resends for an event
            // may be created.
            Err(AsyncInsertError::DatabaseError(
                DieselError::DatabaseError(
                    DatabaseErrorKind::UniqueViolation,
                    info,
                ),
            )) if info.constraint_name()
                == Some("one_webhook_event_dispatch_per_rx") =>
            {
                debug_assert_eq!(trigger, WebhookDeliveryTrigger::Dispatch);
                Ok(None)
            }

            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::pub_test_utils::TestDatabase;
    use nexus_types::external_api::params;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_test_utils::dev;

    #[tokio::test]
    async fn test_dispatched_deliveries_are_unique_per_rx() {
        // Test setup
        let logctx =
            dev::test_setup_log("test_dispatched_deliveries_are_unique_per_rx");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        // As webhook receivers are a collection that owns the delivery
        // resource, we must create a "real" receiver before assigning
        // deliveries to it.
        let rx = datastore
            .webhook_rx_create(
                opctx,
                params::WebhookCreate {
                    identity: IdentityMetadataCreateParams {
                        name: "test-webhook".parse().unwrap(),
                        description: String::new(),
                    },
                    endpoint: "http://webhooks.example.com".parse().unwrap(),
                    secrets: vec!["my cool secret".to_string()],
                    events: vec!["test.*".to_string()],
                    disable_probes: false,
                },
            )
            .await
            .unwrap();
        let rx_id = rx.rx.identity.id.into();
        let conn = datastore.pool_connection_for_tests().await.unwrap();

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

        let dispatch1 = datastore
            .webhook_event_insert_delivery_on_conn(
                &event,
                &rx_id,
                WebhookDeliveryTrigger::Dispatch,
                &*conn,
            )
            .await
            .expect("dispatch 1 should insert");
        assert!(
            dispatch1.is_some(),
            "the first dispatched delivery of an event should be created"
        );

        let dispatch2 = datastore
            .webhook_event_insert_delivery_on_conn(
                &event,
                &rx_id,
                WebhookDeliveryTrigger::Dispatch,
                &*conn,
            )
            .await
            .expect("dispatch 2 insert should not fail");
        assert_eq!(
            dispatch2, None,
            "dispatching an event a second time should do nothing"
        );

        let resend1 = datastore
            .webhook_event_insert_delivery_on_conn(
                &event,
                &rx_id,
                WebhookDeliveryTrigger::Resend,
                &*conn,
            )
            .await
            .expect("resend 1 insert should not fail");
        assert!(
            resend1.is_some(),
            "resending an event should create a new delivery"
        );

        let resend2 = datastore
            .webhook_event_insert_delivery_on_conn(
                &event,
                &rx_id,
                WebhookDeliveryTrigger::Resend,
                &*conn,
            )
            .await
            .expect("resend  insert should not fail");
        assert!(
            resend2.is_some(),
            "resending an event a second time should create a new delivery"
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
