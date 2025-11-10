// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods for webhook event deliveries

use super::DataStore;
use crate::context::OpContext;
use crate::db::IncompleteOnConflictExt;
use crate::db::datastore::RunnableQuery;
use crate::db::model::Alert;
use crate::db::model::AlertClass;
use crate::db::model::AlertDeliveryState;
use crate::db::model::AlertDeliveryTrigger;
use crate::db::model::WebhookDelivery;
use crate::db::model::WebhookDeliveryAttempt;
use crate::db::model::WebhookDeliveryAttemptResult;
use crate::db::pagination::paginated_multicolumn;
use crate::db::update_and_check::UpdateAndCheck;
use crate::db::update_and_check::UpdateAndQueryResult;
use crate::db::update_and_check::UpdateStatus;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::TimeDelta;
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_schema::schema;
use nexus_db_schema::schema::alert::dsl as alert_dsl;
use nexus_db_schema::schema::webhook_delivery::dsl;
use nexus_db_schema::schema::webhook_delivery_attempt::dsl as attempt_dsl;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_uuid_kinds::AlertReceiverUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
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

/// A record from the [`WebhookDelivery`] table along with the event class and
/// data of the corresponding [`Alert`] record.
#[derive(Debug, Clone)]
pub struct DeliveryAndEvent {
    pub delivery: WebhookDelivery,
    pub alert_class: AlertClass,
    pub event: serde_json::Value,
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
            .on_conflict((dsl::alert_id, dsl::rx_id))
            .as_partial_index()
            .do_nothing()
            .execute_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Returns a list of all permanently-failed deliveries which are eligible
    /// to resend should a liveness probe with `resend=true` succeed.
    pub async fn webhook_rx_list_resendable_events(
        &self,
        opctx: &OpContext,
        rx_id: &AlertReceiverUuid,
    ) -> ListResultVec<Alert> {
        Self::rx_list_resendable_events_query(*rx_id)
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    fn rx_list_resendable_events_query(
        rx_id: AlertReceiverUuid,
    ) -> impl RunnableQuery<Alert> {
        use diesel::dsl::*;
        let (delivery, also_delivery) = diesel::alias!(
            schema::webhook_delivery as delivery,
            schema::webhook_delivery as also_delivery
        );
        alert_dsl::alert
            .filter(alert_dsl::alert_class.ne(AlertClass::Probe))
            .inner_join(
                delivery.on(delivery.field(dsl::alert_id).eq(alert_dsl::id)),
            )
            .filter(delivery.field(dsl::rx_id).eq(rx_id.into_untyped_uuid()))
            .filter(not(exists(
                also_delivery
                    .select(also_delivery.field(dsl::id))
                    .filter(
                        also_delivery.field(dsl::alert_id).eq(alert_dsl::id),
                    )
                    .filter(
                        also_delivery
                            .field(dsl::state)
                            .ne(AlertDeliveryState::Failed),
                    )
                    .filter(
                        also_delivery
                            .field(dsl::triggered_by)
                            .ne(AlertDeliveryTrigger::Probe),
                    ),
            )))
            .select(Alert::as_select())
            // the inner join means we may return the same event multiple times,
            // so only return distinct events.
            .distinct()
    }

    pub async fn webhook_rx_delivery_list(
        &self,
        opctx: &OpContext,
        rx_id: &AlertReceiverUuid,
        triggers: &'static [AlertDeliveryTrigger],
        only_states: Vec<AlertDeliveryState>,
        pagparams: &DataPageParams<'_, (DateTime<Utc>, Uuid)>,
    ) -> ListResultVec<(WebhookDelivery, AlertClass, Vec<WebhookDeliveryAttempt>)>
    {
        let conn = self.pool_connection_authorized(opctx).await?;
        // Paginate the query, ordered by delivery UUID.
        let mut query = paginated_multicolumn(
            dsl::webhook_delivery,
            (dsl::time_created, dsl::id),
            pagparams,
        )
        // Select only deliveries that are to the receiver we're interested in,
        // and were initiated by the triggers we're interested in.
        .filter(
            dsl::rx_id
                .eq(rx_id.into_untyped_uuid())
                .and(dsl::triggered_by.eq_any(triggers)),
        )
        // Join with the event table on the delivery's event ID,
        // so that we can grab the event class of the event that initiated
        // this delivery.
        .inner_join(alert_dsl::alert.on(dsl::alert_id.eq(alert_dsl::id)));
        if !only_states.is_empty() {
            query = query.filter(dsl::state.eq_any(only_states));
        }

        let deliveries = query
            .select((WebhookDelivery::as_select(), alert_dsl::alert_class))
            .load_async::<(WebhookDelivery, AlertClass)>(&*conn)
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
        rx_id: &AlertReceiverUuid,
        cfg: &DeliveryConfig,
    ) -> Result<
        impl ExactSizeIterator<Item = DeliveryAndEvent> + 'static + use<>,
        Error,
    > {
        let conn = self.pool_connection_authorized(opctx).await?;
        let now =
            diesel::dsl::now.into_sql::<diesel::pg::sql_types::Timestamptz>();
        let rows = dsl::webhook_delivery
            // Filter out deliveries triggered by probe requests, as those are
            // executed synchronously by the probe endpoint, rather than by the
            // webhook deliverator.
            .filter(dsl::triggered_by.ne(AlertDeliveryTrigger::Probe))
            // Only select deliveries that are still in progress.
            .filter(
                dsl::time_completed
                    .is_null()
                    .and(dsl::state.eq(AlertDeliveryState::Pending)),
            )
            .filter(dsl::rx_id.eq(rx_id.into_untyped_uuid()))
            .filter((dsl::deliverator_id.is_null()).or(
                dsl::time_leased.is_not_null().and(
                    dsl::time_leased.le(now.nullable() - cfg.lease_timeout),
                ),
            ))
            .filter(
                // Retry backoffs: one of the following must be true:
                // - the delivery has not yet been attempted,
                dsl::attempts
                    .eq(0)
                    // - this is the first retry and the previous attempt was at
                    //   least `first_retry_backoff` ago, or
                    .or(dsl::attempts.eq(1).and(
                        dsl::time_leased
                            .le(now.nullable() - cfg.first_retry_backoff),
                    ))
                    // - this is the second retry, and the previous attempt was at
                    //   least `second_retry_backoff` ago.
                    .or(dsl::attempts.eq(2).and(
                        dsl::time_leased
                            .le(now.nullable() - cfg.second_retry_backoff),
                    )),
            )
            .order_by(dsl::time_created.asc())
            // Join with the `webhook_event` table to get the event class, which
            // is necessary to construct delivery requests.
            .inner_join(alert_dsl::alert.on(alert_dsl::id.eq(dsl::alert_id)))
            .select((
                WebhookDelivery::as_select(),
                alert_dsl::alert_class,
                alert_dsl::payload,
            ))
            .load_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        Ok(rows.into_iter().map(|(delivery, alert_class, event)| {
            DeliveryAndEvent { delivery, alert_class, event }
        }))
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
        let UpdateAndQueryResult { found, status } =
            diesel::update(dsl::webhook_delivery)
                .filter(
                    dsl::time_completed
                        .is_null()
                        .and(dsl::state.eq(AlertDeliveryState::Pending)),
                )
                .filter(dsl::id.eq(id))
                .filter(dsl::deliverator_id.is_null().or(
                    dsl::time_leased.is_not_null().and(
                        dsl::time_leased.le(now.nullable() - lease_timeout),
                    ),
                ))
                .set((
                    dsl::time_leased.eq(now.nullable()),
                    dsl::deliverator_id.eq(nexus_id.into_untyped_uuid()),
                ))
                .check_if_exists::<WebhookDelivery>(id)
                .execute_and_check(&conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;
        match status {
            UpdateStatus::Updated => Ok(DeliveryAttemptState::Started),
            UpdateStatus::NotUpdatedButExists => {
                if let Some(completed) = found.time_completed {
                    return Ok(DeliveryAttemptState::AlreadyCompleted(
                        completed,
                    ));
                }

                if let Some(started) = found.time_leased {
                    let nexus_id = found.deliverator_id.ok_or_else(|| {
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

                // Something else prevented this delivery from being leased; log
                // the state of the delivery record, as this is unexpected.
                const MSG: &'static str = "found an incomplete webhook \
                     delivery which is not leased by another Nexus, but \
                     which was not in a consistent state to be leased";
                slog::error!(
                    opctx.log,
                    "couldn't start delivery attempt: {MSG}";
                    "delivery_id" => %id,
                    "alert_id" => %delivery.alert_id,
                    "nexus_id" => %nexus_id,
                    "found_deliverator_id" => ?found.deliverator_id,
                    "found_time_leased" => ?found.time_leased,
                    "found_time_completed" => ?found.time_completed,
                    "found_state" => ?found.state,
                    "found_attempts" => found.attempts.0,
                );
                Err(Error::internal_error(MSG))
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

        // Has the delivery either completed successfully or exhausted all of
        // its retry attempts?
        let new_state =
            if attempt.result == WebhookDeliveryAttemptResult::Succeeded {
                // The delivery has completed successfully.
                AlertDeliveryState::Delivered
            } else if *attempt.attempt >= MAX_ATTEMPTS {
                // The delivery attempt failed, and we are out of retries. This
                // delivery has failed permanently.
                AlertDeliveryState::Failed
            } else {
                // This delivery attempt failed, but we still have retries
                // remaining, so the delivery remains pending.
                AlertDeliveryState::Pending
            };
        let (completed, new_nexus_id) =
            if new_state != AlertDeliveryState::Pending {
                // If the delivery has succeeded or failed permanently, set the
                // "time_completed" timestamp to mark it as finished. Also, leave
                // the delivering Nexus ID in place to maintain a record of who
                // finished the delivery.
                (Some(Utc::now()), Some(nexus_id.into_untyped_uuid()))
            } else {
                // Otherwise, "unlock" the delivery for other nexii.
                (None, None)
            };

        // First, insert the attempt record.
        diesel::insert_into(attempt_dsl::webhook_delivery_attempt)
            .values(attempt.clone())
            .execute_async(&*conn)
            .await
            .map_err(|e| {
                public_error_from_diesel(e, ErrorHandler::Server)
                    .internal_context(
                        "failed to insert delivery attempt record",
                    )
            })?;

        // Next, update the delivery record based on the outcome of this attempt.
        let UpdateAndQueryResult { status, found } =
            diesel::update(dsl::webhook_delivery)
                .filter(dsl::id.eq(delivery.id.into_untyped_uuid()))
                .filter(dsl::deliverator_id.eq(nexus_id.into_untyped_uuid()))
                // Don't mark a delivery as completed if it's already completed!
                .filter(
                    dsl::time_completed
                        .is_null()
                        .and(dsl::state.eq(AlertDeliveryState::Pending)),
                )
                .set((
                    dsl::state.eq(new_state),
                    dsl::time_completed.eq(completed),
                    dsl::attempts.eq(dsl::attempts + 1),
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

        if found.time_completed.is_some()
            || found.state != AlertDeliveryState::Pending
        {
            return Err(Error::conflict(
                "delivery was already marked as completed",
            ));
        }

        if let Some(other_nexus_id) = found.deliverator_id {
            if other_nexus_id.as_untyped_uuid() != nexus_id.as_untyped_uuid() {
                return Err(Error::conflict(format!(
                    "cannot mark delivery completed, as {other_nexus_id:?} was \
                     attempting to deliver it",
                )));
            }
        }

        // Something else prevented this delivery from being finished; log
        // the state of the delivery record, as this is unexpected.
        const MSG: &'static str = "could not finish webhook delivery attempt \
            for an unexpected reason";
        slog::error!(
            opctx.log,
            "{MSG}";
            "delivery_id" => %delivery.id,
            "alert_id" => %delivery.alert_id,
            "nexus_id" => %nexus_id,
            "found_deliverator_id" => ?found.deliverator_id,
            "found_time_leased" => ?found.time_leased,
            "found_time_completed" => ?found.time_completed,
            "found_state" => ?found.state,
            "found_attempts" => found.attempts.0,
        );

        Err(Error::internal_error(MSG))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::explain::ExplainableAsync;
    use crate::db::pagination::Paginator;
    use crate::db::pub_test_utils::TestDatabase;
    use crate::db::raw_query_builder::expectorate_query_contents;
    use nexus_types::external_api::params;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::AlertUuid;

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
                    subscriptions: vec![
                        "test.*".to_string().try_into().unwrap(),
                    ],
                },
            )
            .await
            .unwrap();
        let rx_id = rx.rx.identity.id.into();
        let alert_id = AlertUuid::new_v4();
        datastore
            .alert_create(
                &opctx,
                alert_id,
                AlertClass::TestFoo,
                serde_json::json!({
                    "answer": 42,
                }),
            )
            .await
            .expect("can't create ye event");

        let dispatch1 = WebhookDelivery::new(
            &alert_id,
            &rx_id,
            AlertDeliveryTrigger::Alert,
        );
        let inserted = datastore
            .webhook_delivery_create_batch(&opctx, vec![dispatch1.clone()])
            .await
            .expect("dispatch 1 should insert");
        assert_eq!(inserted, 1, "first dispatched delivery should be created");

        let dispatch2 = WebhookDelivery::new(
            &alert_id,
            &rx_id,
            AlertDeliveryTrigger::Alert,
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
            &alert_id,
            &rx_id,
            AlertDeliveryTrigger::Resend,
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
            &alert_id,
            &rx_id,
            AlertDeliveryTrigger::Resend,
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
        let mut paginator = Paginator::new(
            crate::db::datastore::SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        while let Some(p) = paginator.next() {
            let deliveries = datastore
                .webhook_rx_delivery_list(
                    &opctx,
                    &rx_id,
                    AlertDeliveryTrigger::ALL,
                    Vec::new(),
                    &p.current_pagparams(),
                )
                .await
                .unwrap();
            paginator = p.found_batch(&deliveries, &|(d, _, _)| {
                (d.time_created, *d.id.as_untyped_uuid())
            });
            all_deliveries
                .extend(deliveries.into_iter().map(|(d, _, _)| dbg!(d).id));
        }

        assert!(all_deliveries.contains(&dispatch1.id));
        assert!(!all_deliveries.contains(&dispatch2.id));
        assert!(all_deliveries.contains(&resend1.id));
        assert!(all_deliveries.contains(&resend2.id));

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn expectorate_rx_list_resendable() {
        let query = DataStore::rx_list_resendable_events_query(
            AlertReceiverUuid::nil(),
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
            AlertReceiverUuid::nil(),
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
