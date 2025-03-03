// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task that dispatches queued webhook events to receivers.

use crate::app::background::Activator;
use crate::app::background::BackgroundTask;
use futures::future::BoxFuture;
use nexus_db_model::WebhookDelivery;
use nexus_db_model::WebhookDeliveryTrigger;
use nexus_db_model::SCHEMA_VERSION;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::datastore::SQL_BATCH_SIZE;
use nexus_db_queries::db::pagination::Paginator;
use nexus_db_queries::db::DataStore;
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
                        // anyuthing interesting today
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
                "event_id" => ?event.id,
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
                        "event_id" => ?event.id,
                        "event_class" => %event.event_class,
                        "error" => &error,
                    );
                    status.errors.push(format!(
                        "{MSG} {} ({}): {error}",
                        event.id, event.event_class
                    ));
                    // We weren't able to find receivers for this event, so
                    // *don't* mark it as dispatched --- it's someone else's
                    // problem now.
                    continue;
                }
            };

            let deliveries: Vec<WebhookDelivery> = rxs.into_iter().map(|(rx, sub)| {
                slog::trace!(&opctx.log, "webhook receiver is subscribed to event";
                    "rx_name" => %rx.name(),
                    "rx_id" => ?rx.id(),
                    "event_id" => ?event.id,
                    "event_class" => %event.event_class,
                    "glob" => ?sub.glob,
                );
                WebhookDelivery::new(&event, &rx.id(), WebhookDeliveryTrigger::Event)
            }).collect();

            let subscribed = if !deliveries.is_empty() {
                let subscribed = deliveries.len();
                let dispatched = match self
                    .datastore
                    .webhook_delivery_create_batch(&opctx, deliveries)
                    .await
                {
                    Ok(created) => created,
                    Err(error) => {
                        slog::error!(&opctx.log, "failed to insert webhook deliveries";
                            "event_id" => ?event.id,
                            "event_class" => %event.event_class,
                            "error" => %error,
                            "num_subscribed" => ?subscribed,
                        );
                        status.errors.push(format!("failed to insert {subscribed} webhook deliveries for event {} ({}): {error}", event.id, event.event_class));
                        // We weren't able to create deliveries for this event, so
                        // *don't* mark it as dispatched.
                        continue;
                    }
                };
                status.dispatched.push(WebhookDispatched {
                    event_id: event.id.into(),
                    subscribed,
                    dispatched,
                });
                slog::debug!(
                    &opctx.log,
                    "dispatched webhook event";
                    "event_id" => ?event.id,
                    "event_class" => %event.event_class,
                    "num_subscribed" => subscribed,
                    "num_dispatched" => dispatched,
                );
                subscribed
            } else {
                slog::debug!(
                    &opctx.log,
                    "no webhook receivers subscribed to event";
                    "event_id" => ?event.id,
                    "event_class" => %event.event_class,
                );
                status.no_receivers.push(event.id.into());
                0
            };

            if let Err(error) = self
                .datastore
                .webhook_event_mark_dispatched(
                    &opctx,
                    &event.id.into(),
                    subscribed,
                )
                .await
            {
                slog::error!(&opctx.log, "failed to mark webhook event as dispatched";
                    "event_id" => ?event.id,
                    "event_class" => %event.event_class,
                    "error" => %error,
                    "num_subscribed" => subscribed,
                );
                status.errors.push(format!("failed to mark webhook event {} ({}) as dispatched: {error}", event.id, event.event_class));
            }
        }
        Ok(())
    }
}
