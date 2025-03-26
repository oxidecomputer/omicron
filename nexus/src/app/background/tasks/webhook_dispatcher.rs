// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task that dispatches queued webhook events to receivers.
//!
//! This task reads un-dispatched webhook events from the [`WebhookEvent`]
//! table, determines which webhook receivers are subscribed to those events,
//! and constructs the event payload for those receivers.  It then inserts new
//! records into the [`WebhookDelivery`] table for those deliveries, which are
//! read by the [`webhook_deliverator`] task that actually sends HTTP requests to
//! receivers.  Prior to dispatching events, this task will first ensure that
//! all webhook receiver glob subscriptions are up to date with the current
//! database schema version, ensuring that receivers with glob subscriptions
//! will receive newly-added event classes that match their globs.
//!
//! For an overview of all the components of the webhook subsystem, their roles,
//! and how they fit together, refer to the comments in the [`app::webhook`]
//! module.
//!
//! [`WebhookEvent`]: nexus_db_model::WebhookEvent
//! [`webhook_deliverator`]: super::webhook_deliverator
//! [`app::webhook`]: crate::app::webhook

use crate::app::background::Activator;
use crate::app::background::BackgroundTask;
use futures::future::BoxFuture;
use nexus_db_model::SCHEMA_VERSION;
use nexus_db_model::WebhookDelivery;
use nexus_db_model::WebhookDeliveryTrigger;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore::SQL_BATCH_SIZE;
use nexus_db_queries::db::pagination::Paginator;
use nexus_types::identity::Asset;
use nexus_types::identity::Resource;
use nexus_types::internal_api::background::{
    WebhookDispatched, WebhookDispatcherStatus, WebhookGlobStatus,
};
use omicron_common::api::external::Error;
use omicron_uuid_kinds::GenericUuid;
use std::sync::Arc;

pub struct WebhookDispatcher {
    datastore: Arc<DataStore>,
    deliverator: Activator,
}

impl BackgroundTask for WebhookDispatcher {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        Box::pin(async move {
            let mut status = WebhookDispatcherStatus {
                globs_reprocessed: Default::default(),
                glob_version: SCHEMA_VERSION,
                dispatched: Vec::new(),
                errors: Vec::new(),
                no_receivers: Vec::new(),
            };
            match self.actually_activate(&opctx, &mut status).await {
                Ok(_) if status.errors.is_empty() => {
                    const MSG: &str =
                        "webhook dispatching completed successfully";
                    if !status.dispatched.is_empty() {
                        slog::info!(
                            &opctx.log,
                            "{MSG}";
                            "events_dispatched" => status.dispatched.len(),
                            "events_without_receivers" => status.no_receivers.len(),
                        );
                    } else {
                        // no sense cluttering up the logs if we didn't do
                        // anything interesting today
                        slog::trace!(
                            &opctx.log,
                            "{MSG}";
                            "events_dispatched" => status.dispatched.len(),
                            "events_without_receivers" => status.no_receivers.len(),
                        );
                    };
                }
                Ok(_) => {
                    slog::warn!(
                        &opctx.log,
                        "webhook dispatching completed with errors";
                        "events_dispatched" => status.dispatched.len(),
                        "events_without_receivers" => status.no_receivers.len(),
                        "events_failed" => status.errors.len(),
                    );
                }
                Err(error) => {
                    slog::error!(
                        &opctx.log,
                        "webhook dispatching failed";
                        "events_dispatched" => status.dispatched.len(),
                        "events_without_receivers" => status.no_receivers.len(),
                        "events_failed" => status.errors.len(),
                        "error" => &error,
                    );
                    status.errors.push(error.to_string());
                }
            };

            // If any new deliveries were dispatched, call the deliverator!
            if !status.dispatched.is_empty() {
                self.deliverator.activate();
            }

            serde_json::json!(status)
        })
    }
}

impl WebhookDispatcher {
    pub fn new(datastore: Arc<DataStore>, deliverator: Activator) -> Self {
        Self { datastore, deliverator }
    }

    async fn actually_activate(
        &mut self,
        opctx: &OpContext,
        status: &mut WebhookDispatcherStatus,
    ) -> Result<(), Error> {
        // Before dispatching any events, ensure that all webhook globs are up
        // to date with the current schema version.
        let mut paginator = Paginator::new(SQL_BATCH_SIZE);
        let mut globs_reprocessed = 0;
        let mut globs_failed = 0;
        let mut globs_already_reprocessed = 0;
        while let Some(p) = paginator.next() {
            let batch = self
                .datastore
                .webhook_glob_list_outdated(opctx, &p.current_pagparams())
                .await
                .map_err(|e| {
                    e.internal_context("failed to list outdated webhook globs")
                })?;
            paginator = p.found_batch(&batch, &|glob| {
                (glob.rx_id.into_untyped_uuid(), glob.glob.glob.clone())
            });

            slog::trace!(
                &opctx.log,
                "reprocessing {} outdated webhook globs...",
                batch.len(),
            );

            for glob in batch {
                let result = self
                    .datastore
                    .webhook_glob_reprocess(opctx, &glob)
                    .await
                    .map_err(|e| {
                        globs_failed += 1;
                        slog::warn!(
                            &opctx.log,
                            "failed to reprocess webhook glob";
                            "rx_id" => ?glob.rx_id,
                            "glob" => ?glob.glob.glob,
                            "glob_version" => %glob.schema_version.0,
                            "error" => %e,
                        );
                        e.to_string()
                    })
                    .inspect(|status| match status {
                        WebhookGlobStatus::Reprocessed { .. } => {
                            globs_reprocessed += 1
                        }
                        WebhookGlobStatus::AlreadyReprocessed => {
                            globs_already_reprocessed += 1
                        }
                    });
                let rx_statuses = status
                    .globs_reprocessed
                    .entry(glob.rx_id.into())
                    .or_default();
                rx_statuses.insert(glob.glob.glob, result);
            }
        }
        if globs_failed > 0 {
            slog::warn!(
                &opctx.log,
                "webhook glob reprocessing completed with failures";
                "globs_failed" => ?globs_failed,
                "globs_reprocessed" => ?globs_reprocessed,
                "globs_already_reprocessed" => ?globs_already_reprocessed,
            );
        } else if globs_reprocessed > 0 {
            slog::info!(
                &opctx.log,
                "webhook glob reprocessed";
                "globs_reprocessed" => ?globs_reprocessed,
                "globs_already_reprocessed" => ?globs_already_reprocessed,
            );
        }

        // Select the next event that has yet to be dispatched in order of
        // creation, until there are none left in need of dispatching.
        while let Some(event) =
            self.datastore.webhook_event_select_next_for_dispatch(opctx).await?
        {
            slog::trace!(
                &opctx.log,
                "dispatching webhook event...";
                "event_id" => ?event.id(),
                "event_class" => %event.event_class,
            );

            // Okay, we found an event that needs to be dispatched. Next, get
            // list the webhook receivers subscribed to this event class and
            // create delivery records for them.
            let rxs = match self
                .datastore
                .webhook_rx_list_subscribed_to_event(&opctx, event.event_class)
                .await
            {
                Ok(rxs) => rxs,
                Err(error) => {
                    const MSG: &str =
                        "failed to list webhook receivers subscribed to event";
                    slog::error!(
                        &opctx.log,
                        "{MSG}";
                        "event_id" => ?event.id(),
                        "event_class" => %event.event_class,
                        "error" => &error,
                    );
                    status.errors.push(format!(
                        "{MSG} {} ({}): {error}",
                        event.id(),
                        event.event_class
                    ));
                    // We weren't able to find receivers for this event, so
                    // *don't* mark it as dispatched --- it's someone else's
                    // problem now.
                    continue;
                }
            };

            let deliveries: Vec<WebhookDelivery> = rxs
                .into_iter()
                .map(|(rx, sub)| {
                    // NOTE: In the future, if we add support for webhook receivers
                    // with roles other than 'fleet.viewer' (as described in
                    // https://rfd.shared.oxide.computer/rfd/538#rbac-filtering),
                    // this might be where we filter the actual dispatched payload
                    // based on the individual receiver's permissions.
                    slog::trace!(
                        &opctx.log,
                        "webhook receiver is subscribed to event";
                        "rx_name" => %rx.name(),
                        "rx_id" => ?rx.id(),
                        "event_id" => ?event.id(),
                        "event_class" => %event.event_class,
                        "glob" => ?sub.glob,
                    );
                    WebhookDelivery::new(
                        &event.id(),
                        &rx.id(),
                        WebhookDeliveryTrigger::Event,
                    )
                })
                .collect();

            let subscribed = if !deliveries.is_empty() {
                let subscribed = deliveries.len();
                let dispatched = match self
                    .datastore
                    .webhook_delivery_create_batch(&opctx, deliveries)
                    .await
                {
                    Ok(created) => created,
                    Err(error) => {
                        slog::error!(&opctx.log,
                            "failed to insert webhook deliveries";
                            "event_id" => ?event.id(),
                            "event_class" => %event.event_class,
                            "error" => %error,
                            "num_subscribed" => ?subscribed,
                        );
                        status.errors.push(format!(
                            "failed to insert {subscribed} webhook deliveries \
                             for event {} ({}): {error}",
                            event.id(),
                            event.event_class,
                        ));
                        // We weren't able to create deliveries for this event, so
                        // *don't* mark it as dispatched.
                        continue;
                    }
                };
                status.dispatched.push(WebhookDispatched {
                    event_id: event.id(),
                    subscribed,
                    dispatched,
                });
                slog::debug!(
                    &opctx.log,
                    "dispatched webhook event";
                    "event_id" => ?event.id(),
                    "event_class" => %event.event_class,
                    "num_subscribed" => subscribed,
                    "num_dispatched" => dispatched,
                );
                subscribed
            } else {
                slog::debug!(
                    &opctx.log,
                    "no webhook receivers subscribed to event";
                    "event_id" => ?event.id(),
                    "event_class" => %event.event_class,
                );
                status.no_receivers.push(event.id());
                0
            };

            if let Err(error) = self
                .datastore
                .webhook_event_mark_dispatched(&opctx, &event.id(), subscribed)
                .await
            {
                slog::error!(
                    &opctx.log,
                    "failed to mark webhook event as dispatched";
                    "event_id" => ?event.id(),
                    "event_class" => %event.event_class,
                    "error" => %error,
                    "num_subscribed" => subscribed,
                );
                status.errors.push(format!(
                    "failed to mark webhook event {} ({}) as dispatched: \
                     {error}",
                    event.id(),
                    event.event_class,
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use diesel::prelude::*;
    use nexus_db_queries::db;
    use nexus_test_utils_macros::nexus_test;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_uuid_kinds::WebhookEventUuid;
    use omicron_uuid_kinds::WebhookReceiverUuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    // Tests that stale webhook event class globs are reprocessed prior to event
    // dispatching.
    #[nexus_test(server = crate::Server)]
    async fn test_glob_reprocessing(cptestctx: &ControlPlaneTestContext) {
        use nexus_db_model::schema::webhook_receiver::dsl as rx_dsl;
        use nexus_db_model::schema::webhook_rx_event_glob::dsl as glob_dsl;
        use nexus_db_model::schema::webhook_rx_subscription::dsl as subscription_dsl;

        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );
        let rx_id = WebhookReceiverUuid::new_v4();
        let conn = datastore
            .pool_connection_for_tests()
            .await
            .expect("can't get ye pool_connection_for_tests");

        // Unfortunately, we've gotta hand-create the receiver and its
        // subscriptions, so that we can create a set of globs that differs from
        // those generated by the currrent schema.
        diesel::insert_into(rx_dsl::webhook_receiver)
            .values(db::model::WebhookReceiver {
                identity: db::model::WebhookReceiverIdentity::new(
                    rx_id,
                    IdentityMetadataCreateParams {
                        name: "my-cool-webhook".parse().unwrap(),
                        description: "it's my cool webhook".to_string(),
                    },
                ),

                endpoint: "http://webhooks.elizas.website".parse().unwrap(),
                secret_gen: db::model::Generation::new(),
                subscription_gen: db::model::Generation::new(),
            })
            .execute_async(&*conn)
            .await
            .expect("receiver entry should create");

        const GLOB_PATTERN: &str = "test.*.bar";
        let glob = GLOB_PATTERN
            .parse::<db::model::WebhookGlob>()
            .expect("'test.*.bar should be an acceptable glob");
        let mut glob = db::model::WebhookRxEventGlob::new(rx_id, glob);
        // Just make something up that's obviously outdated...
        glob.schema_version = db::model::SemverVersion::new(100, 0, 0);
        diesel::insert_into(glob_dsl::webhook_rx_event_glob)
            .values(glob.clone())
            .execute_async(&*conn)
            .await
            .expect("should insert glob entry");
        diesel::insert_into(subscription_dsl::webhook_rx_subscription)
            .values(
                // Pretend `test.quux.bar` doesn't exist yet
                db::model::WebhookRxSubscription::for_glob(
                    &glob,
                    db::model::WebhookEventClass::TestFooBar,
                ),
            )
            .execute_async(&*conn)
            .await
            .expect("should insert glob entry");
        // Also give the webhook receiver a secret just so everything
        // looks normalish.
        let (authz_rx, _) = db::lookup::LookupPath::new(&opctx, datastore)
            .webhook_receiver_id(rx_id)
            .fetch()
            .await
            .expect("webhook rx should be there");
        datastore
            .webhook_rx_secret_create(
                &opctx,
                &authz_rx,
                db::model::WebhookSecret::new(rx_id, "TRUSTNO1".to_string()),
            )
            .await
            .expect("cant insert ye secret???");

        // OKAY GREAT NOW THAT WE DID ALL THAT STUFF let's see if it actually
        // works...

        // N.B. that we are using the `DataStore::webhook_event_create` method
        // rather than `Nexus::webhook_event_publish` (the expected entrypoint
        // to publishing a webhook event) because `webhook_event_publish` also
        // activates the dispatcher task, and for this test, we would like to be
        // responsible for activating it.
        let event_id = WebhookEventUuid::new_v4();
        datastore
            .webhook_event_create(
                &opctx,
                event_id,
                db::model::WebhookEventClass::TestQuuxBar,
                serde_json::json!({"msg": "help im trapped in a webhook event factory"}),
            )
            .await
            .expect("creating the event should work");

        // okay now do the thing
        let mut status = WebhookDispatcherStatus {
            globs_reprocessed: Default::default(),
            glob_version: SCHEMA_VERSION,
            dispatched: Vec::new(),
            errors: Vec::new(),
            no_receivers: Vec::new(),
        };

        let mut task = WebhookDispatcher::new(
            datastore.clone(),
            nexus.background_tasks.task_webhook_deliverator.clone(),
        );
        task.actually_activate(&opctx, &mut status)
            .await
            .expect("activation should succeed");

        // The globs should have been reprocessed, creating a subscription to
        // `test.quux.bar`.
        let subscriptions = subscription_dsl::webhook_rx_subscription
            .filter(subscription_dsl::rx_id.eq(rx_id.into_untyped_uuid()))
            .load_async::<db::model::WebhookRxSubscription>(&*conn)
            .await
            .expect("should be able to get subscriptions")
            .into_iter()
            .map(|sub| {
                // throw away the "time_created" fields so that assertions are
                // easier...
                assert_eq!(
                    sub.glob.as_deref(),
                    Some(GLOB_PATTERN),
                    "found a subscription to {} that was not from our glob: {sub:?}",
                    sub.event_class,
                );
                sub.event_class
            }).collect::<std::collections::HashSet<_>>();
        assert_eq!(subscriptions.len(), 2);
        assert!(
            subscriptions.contains(&db::model::WebhookEventClass::TestFooBar),
            "subscription to test.foo.bar should exist; subscriptions: \
             {subscriptions:?}",
        );
        assert!(
            subscriptions.contains(&db::model::WebhookEventClass::TestQuuxBar),
            "subscription to test.quux.bar should exist; subscriptions: \
             {subscriptions:?}",
        );
        let rx_reprocessed_globs = status.globs_reprocessed.get(&rx_id).expect(
            "expected there to be an entry in status.globs_reprocessed \
                 for our glob",
        );
        let reprocessed_entry = dbg!(rx_reprocessed_globs).get(GLOB_PATTERN);
        assert!(
            matches!(
                reprocessed_entry,
                Some(Ok(WebhookGlobStatus::Reprocessed { .. }))
            ),
            "glob status should be 'reprocessed'"
        );

        // There should now be a delivery entry for the event we published.
        //
        // Use `webhook_rx_delivery_list` rather than
        // `webhook_rx_delivery_list_ready`, even though it's a bit more
        // complex due to requiring pagination. This is because the
        // webhook_deliverator background task may have activated and might
        // attempt to deliver the event, making it no longer show up in the
        // "ready" query.
        let mut paginator = Paginator::new(db::datastore::SQL_BATCH_SIZE);
        let mut deliveries = Vec::new();
        while let Some(p) = paginator.next() {
            let batch = datastore
                .webhook_rx_delivery_list(
                    &opctx,
                    &rx_id,
                    &[WebhookDeliveryTrigger::Event],
                    Vec::new(),
                    &p.current_pagparams(),
                )
                .await
                .unwrap();
            paginator = p.found_batch(&batch, &|(d, _, _)| {
                (d.time_created, d.id.into_untyped_uuid())
            });
            deliveries.extend(batch);
        }
        let event =
            deliveries.iter().find(|(d, _, _)| d.event_id == event_id.into());
        assert!(
            dbg!(event).is_some(),
            "delivery entry for dispatched event must exist"
        );
    }
}
