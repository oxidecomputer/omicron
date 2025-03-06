// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods for webhook event deliveries

use super::DataStore;
use crate::context::OpContext;
use crate::db::datastore::RunnableQuery;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::model::SqlU8;
use crate::db::model::WebhookDelivery;
use crate::db::model::WebhookDeliveryAttempt;
use crate::db::model::WebhookDeliveryTrigger;
use crate::db::model::WebhookEvent;
use crate::db::model::WebhookEventClass;
use crate::db::pagination::paginated;
use crate::db::schema;
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
use nexus_types::external_api::params::WebhookDeliveryStateFilter;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::WebhookReceiverUuid;
use uuid::Uuid;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DeliveryAttemptState {
    Started,
    AlreadyCompleted(DateTime<Utc>),
    InProgress { nexus_id: OmicronZoneUuid, started: DateTime<Utc> },
}

#[derive(Debug, Clone)]
pub struct DeliveryConfig {
    pub first_retry_backoff: TimeDelta,
    pub second_retry_backoff: TimeDelta,
    pub lease_timeout: TimeDelta,
}

impl Default for DeliveryConfig {
    fn default() -> Self {
        Self {
            lease_timeout: TimeDelta::seconds(60), // 1 minute
            first_retry_backoff: TimeDelta::seconds(60), // 1 minute
            second_retry_backoff: TimeDelta::seconds(60 * 5), // 5 minutes
        }
    }
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

    /// Returns a list of all permanently-failed deliveries which are eligible
    /// to resend should a liveness probe with `resend=true` succeed.
    pub async fn webhook_rx_list_resendable_events(
        &self,
        opctx: &OpContext,
        rx_id: &WebhookReceiverUuid,
    ) -> ListResultVec<WebhookEvent> {
        Self::rx_list_resendable_events_query(*rx_id)
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    fn rx_list_resendable_events_query(
        rx_id: WebhookReceiverUuid,
    ) -> impl RunnableQuery<WebhookEvent> {
        use diesel::dsl::*;
        let (delivery, also_delivery) = diesel::alias!(
            schema::webhook_delivery as delivery,
            schema::webhook_delivery as also_delivey
        );
        event_dsl::webhook_event
            .filter(event_dsl::event_class.ne(WebhookEventClass::Probe))
            .inner_join(
                delivery.on(delivery.field(dsl::event_id).eq(event_dsl::id)),
            )
            .filter(delivery.field(dsl::rx_id).eq(rx_id.into_untyped_uuid()))
            .filter(not(exists(
                also_delivery
                    .select(also_delivery.field(dsl::id))
                    .filter(
                        also_delivery.field(dsl::event_id).eq(event_dsl::id),
                    )
                    .filter(
                        also_delivery.field(dsl::failed_permanently).eq(&false),
                    )
                    .filter(
                        also_delivery
                            .field(dsl::trigger)
                            .ne(WebhookDeliveryTrigger::Probe),
                    ),
            )))
            .select(WebhookEvent::as_select())
            // the inner join means we may return the same event multiple times,
            // so only return distinct events.
            .distinct()
    }

    pub async fn webhook_rx_delivery_list(
        &self,
        opctx: &OpContext,
        rx_id: &WebhookReceiverUuid,
        triggers: &'static [WebhookDeliveryTrigger],
        state_filter: WebhookDeliveryStateFilter,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<(
        WebhookDelivery,
        WebhookEventClass,
        Vec<WebhookDeliveryAttempt>,
    )> {
        let conn = self.pool_connection_authorized(opctx).await?;
        // Paginate the query, ordered by delivery UUID.
        let mut query = paginated(dsl::webhook_delivery, dsl::id, pagparams)
            // Select only deliveries that are to the receiver we're interested in,
            // and were initiated by the triggers we're interested in.
            .filter(
                dsl::rx_id
                    .eq(rx_id.into_untyped_uuid())
                    .and(dsl::trigger.eq_any(triggers)),
            )
            // Join with the event table on the delivery's event ID,
            // so that we can grab the event class of the event that initiated
            // this delivery.
            .inner_join(
                event_dsl::webhook_event.on(dsl::event_id.eq(event_dsl::id)),
            );
        if !state_filter.include_failed() {
            query = query.filter(dsl::failed_permanently.eq(false));
        }
        if !state_filter.include_pending() {
            query = query.filter(dsl::time_completed.is_not_null());
        }
        if !state_filter.include_succeeded() {
            // If successful deliveries are excluded
            query =
                query.filter(dsl::time_completed.is_null().or(
                    dsl::failed_permanently.eq(state_filter.include_failed()),
                ));
        }

        let deliveries = query
            .select((WebhookDelivery::as_select(), event_dsl::event_class))
            .load_async::<(WebhookDelivery, WebhookEventClass)>(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        let mut result = Vec::with_capacity(deliveries.len());
        for (delivery, class) in deliveries {
            let attempts = attempt_dsl::webhook_delivery_attempt
                .filter(
                    attempt_dsl::delivery_id
                        .eq(delivery.id.into_untyped_uuid()),
                )
                .select(WebhookDeliveryAttempt::as_select())
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                        .internal_context(
                            "failed to list attempts for a delivery",
                        )
                })?;
            result.push((delivery, class, attempts));
        }
        Ok(result)
    }

    pub async fn webhook_rx_delivery_list_ready(
        &self,
        opctx: &OpContext,
        rx_id: &WebhookReceiverUuid,
        cfg: &DeliveryConfig,
    ) -> ListResultVec<(WebhookDelivery, WebhookEventClass)> {
        let conn = self.pool_connection_authorized(opctx).await?;
        let now =
            diesel::dsl::now.into_sql::<diesel::pg::sql_types::Timestamptz>();
        dsl::webhook_delivery
            // Filter out deliveries triggered by probe requests, as those are
            // executed synchronously by the probe endpoint, rather than by the
            // webhook deliverator.
            .filter(dsl::trigger.ne(WebhookDeliveryTrigger::Probe))
            .filter(dsl::time_completed.is_null())
            .filter(dsl::rx_id.eq(rx_id.into_untyped_uuid()))
            .filter(
                (dsl::deliverator_id.is_null()).or(dsl::time_delivery_started
                    .is_not_null()
                    .and(
                        dsl::time_delivery_started
                            .le(now.nullable() - cfg.lease_timeout),
                    )),
            )
            .filter(
                // Retry backoffs: one of the following must be true:
                // - the delivery has not yet been attempted,
                dsl::attempts
                    .eq(0)
                    // - this is the first retry and the previous attempt was at
                    //   least `first_retry_backoff` ago, or
                    .or(dsl::attempts.eq(1).and(
                        dsl::time_delivery_started
                            .le(now.nullable() - cfg.first_retry_backoff),
                    ))
                    // - this is the second retry, and the previous attempt was at
                    //   least `second_retry_backoff` ago.
                    .or(dsl::attempts.eq(2).and(
                        dsl::time_delivery_started
                            .le(now.nullable() - cfg.second_retry_backoff),
                    )),
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
        const MAX_ATTEMPTS: u8 = 3;
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
        let failed_permanently = !succeeded && *attempt.attempt >= MAX_ATTEMPTS;
        let (completed, new_nexus_id) = if succeeded || failed_permanently {
            // If the delivery has succeeded or failed permanently, set the
            // "time_completed" timestamp to mark it as finished. Also, leave
            // the delivering Nexus ID in place to maintain a record of who
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
                    dsl::failed_permanently.eq(failed_permanently),
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
    use crate::db::explain::ExplainableAsync;
    use crate::db::model::WebhookDeliveryTrigger;
    use crate::db::pagination::Paginator;
    use crate::db::pub_test_utils::TestDatabase;
    use crate::db::raw_query_builder::expectorate_query_contents;
    use dropshot::PaginationOrder;
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

        let dispatch1 =
            WebhookDelivery::new(&event, &rx_id, WebhookDeliveryTrigger::Event);
        let inserted = datastore
            .webhook_delivery_create_batch(&opctx, vec![dispatch1.clone()])
            .await
            .expect("dispatch 1 should insert");
        assert_eq!(inserted, 1, "first dispatched delivery should be created");

        let dispatch2 =
            WebhookDelivery::new(&event, &rx_id, WebhookDeliveryTrigger::Event);
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

        todo!("ELIZA PUT THIS PART BACK");
        // let mut all_deliveries = std::collections::HashSet::new();
        // let mut paginator =
        //     Paginator::new(crate::db::datastore::SQL_BATCH_SIZE);
        // while let Some(p) = paginator.next() {
        //     let deliveries = datastore
        //         .webhook_rx_delivery_list_attempts(
        //             &opctx,
        //             &rx_id,
        //             WebhookDeliveryTrigger::ALL,
        //             std::iter::once(WebhookDeliveryState::Pending),
        //             &p.current_pagparams(),
        //         )
        //         .await
        //         .unwrap();
        //     paginator = p.found_batch(&deliveries, &|(d, a, _): &(
        //         WebhookDelivery,
        //         Option<WebhookDeliveryAttempt>,
        //         WebhookEventClass,
        //     )| {
        //         (
        //             *d.id.as_untyped_uuid(),
        //             a.as_ref()
        //                 .map(|attempt| attempt.attempt)
        //                 .unwrap_or(SqlU8::new(0)),
        //         )
        //     });
        //     all_deliveries
        //         .extend(deliveries.into_iter().map(|(d, _, _)| dbg!(d).id));
        // }

        // assert!(all_deliveries.contains(&dispatch1.id));
        // assert!(!all_deliveries.contains(&dispatch2.id));
        // assert!(all_deliveries.contains(&resend1.id));
        // assert!(all_deliveries.contains(&resend2.id));

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn expectorate_rx_list_resendable() {
        let query = DataStore::rx_list_resendable_events_query(
            WebhookReceiverUuid::nil(),
        );

        expectorate_query_contents(
            &query,
            "tests/output/webhook_rx_list_resendable_events.sql",
        )
        .await;
    }

    #[tokio::test]
    async fn explain_rx_list_resendable_events() {
        let logctx = dev::test_setup_log("explain_rx_list_resendable_events");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        let query = DataStore::rx_list_resendable_events_query(
            WebhookReceiverUuid::nil(),
        );
        let explanation = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");

        eprintln!("{explanation}");

        assert!(
            !explanation.contains("FULL SCAN"),
            "Found an unexpected FULL SCAN: {}",
            explanation
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
