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
use crate::db::pagination::paginated;
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
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_uuid_kinds::{GenericUuid, OmicronZoneUuid, WebhookReceiverUuid};
use uuid::Uuid;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DeliveryAttemptState {
    Started,
    AlreadyCompleted(DateTime<Utc>),
    InProgress { nexus_id: OmicronZoneUuid, started: DateTime<Utc> },
}

impl DataStore {
    pub async fn webhook_delivery_create_batch(
        &self,
        opctx: &OpContext,
        deliveries: Vec<WebhookDelivery>,
    ) -> CreateResult<usize> {
        let conn = self.pool_connection_authorized(opctx).await?;
        diesel::insert_into(dsl::webhook_delivery)
            .values(deliveries)
            // N.B. that this is intended to ignore conflicts on the
            // "one_webhook_event_dispatch_per_rx" index, but ON CONFLICT ... DO
            // NOTHING can't be used with the names of indices, only actual
            // UNIQUE CONSTRAINTs. So we just do a blanket ON CONFLICT DO
            // NOTHING, which is fine, becausse the only other uniqueness
            // constraint is the UUID primary key, and we kind of assume UUID
            // collisions don't happen. Oh well.
            .on_conflict_do_nothing()
            .execute_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn webhook_rx_delivery_list(
        &self,
        opctx: &OpContext,
        rx_id: &WebhookReceiverUuid,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<WebhookDelivery> {
        let conn = self.pool_connection_authorized(opctx).await?;
        paginated(dsl::webhook_delivery, dsl::id, pagparams)
            .filter(dsl::rx_id.eq(rx_id.into_untyped_uuid()))
            .load_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

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

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::model::WebhookDeliveryTrigger;
    use crate::db::pagination::Paginator;
    use crate::db::pub_test_utils::TestDatabase;
    use nexus_types::external_api::params;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::WebhookEventUuid;

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

        let dispatch1 = WebhookDelivery::new(
            &event,
            &rx_id,
            WebhookDeliveryTrigger::Dispatch,
        );
        let inserted = datastore
            .webhook_delivery_create_batch(&opctx, vec![dispatch1.clone()])
            .await
            .expect("dispatch 1 should insert");
        assert_eq!(inserted, 1, "first dispatched delivery should be created");

        let dispatch2 = WebhookDelivery::new(
            &event,
            &rx_id,
            WebhookDeliveryTrigger::Dispatch,
        );
        let inserted = datastore
            .webhook_delivery_create_batch(opctx, vec![dispatch2.clone()])
            .await
            .expect("dispatch 2 insert should not fail");
        assert_eq!(
            inserted, 0,
            "dispatching an event a second time should do nothing"
        );

        let resend1 = WebhookDelivery::new(
            &event,
            &rx_id,
            WebhookDeliveryTrigger::Resend,
        );
        let inserted = datastore
            .webhook_delivery_create_batch(opctx, vec![resend1.clone()])
            .await
            .expect("resend 1 insert should not fail");
        assert_eq!(
            inserted, 1,
            "resending an event should create a new delivery"
        );

        let resend2 = WebhookDelivery::new(
            &event,
            &rx_id,
            WebhookDeliveryTrigger::Resend,
        );
        let inserted = datastore
            .webhook_delivery_create_batch(opctx, vec![resend2.clone()])
            .await
            .expect("resend 2 insert should not fail");
        assert_eq!(
            inserted, 1,
            "resending an event a second time should create a new delivery"
        );

        let mut all_deliveries = std::collections::HashSet::new();
        let mut paginator =
            Paginator::new(crate::db::datastore::SQL_BATCH_SIZE);
        while let Some(p) = paginator.next() {
            let deliveries = datastore
                .webhook_rx_delivery_list(
                    &opctx,
                    &rx_id,
                    &p.current_pagparams(),
                )
                .await
                .unwrap();
            paginator = p.found_batch(&deliveries, &|d: &WebhookDelivery| {
                d.id.into_untyped_uuid()
            });
            all_deliveries.extend(deliveries.into_iter().map(|d| d.id));
        }

        assert!(all_deliveries.contains(&dispatch1.id));
        assert!(!all_deliveries.contains(&dispatch2.id));
        assert!(all_deliveries.contains(&resend1.id));
        assert!(all_deliveries.contains(&resend2.id));

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
