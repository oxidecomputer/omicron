// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task that sends HTTP requests to webhook receiver endpoints for
//! active webhook deliveries.
//!
//! This task reads [`WebhookDelivery`] records from the database (created by the
//! [`alert_dispatcher`] task) and sends HTTP requests to the receivers for
//! those records.  The deliverator is responsible for recording the status of
//! each of these attempts, and for retrying failed attempts as needed.  For
//! an overview of all the components of the webhook subsystem, their roles, and
//! how they fit together, refer to the comments in the [`app::webhook`] module.
//!
//! The `webhook_deliverator` background tasks in multiple Nexus instances
//! coordinate between each other using a simple advisory lease system to avoid
//! attempting to deliver the same event to the same receiver simultaneously.
//! Since webhooks guarantee at-least-once delivery, this mechanism is not
//! required for correctness; instead, it serves only to reduce unnecessary work
//! by avoiding duplicate delivereis when possible.  A delivery is leased by
//! setting the `deliverator_id` field to the leasing Nexus' Omicron zone UUID
//! and the `time_leased` field to the current timestamp.  When querying the
//! database for deliveries in need of deliverating, a delivery is selected if
//! its `deliverator_id` field is unset OR if a configurable timeout period has
//! elapsed since its `time_leased` timestamp.  This way, should a Nexus
//! instance die or get stuck in the midst of a delivery, its lease will
//! eventually time out, and other Nexii will attempt that delivery.
//!
//! [`WebhookDelivery`]: nexus_db_model::WebhookDelivery
//! [`alert_dispatcher`]: super::alert_dispatcher
//! [`app::webhook`]: crate::app::webhook

use crate::app::background::BackgroundTask;
use crate::app::webhook::ReceiverClient;
use futures::future::BoxFuture;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore::webhook_delivery::DeliveryAndEvent;
use nexus_db_queries::db::datastore::webhook_delivery::DeliveryAttemptState;
pub use nexus_db_queries::db::datastore::webhook_delivery::DeliveryConfig;
use nexus_db_queries::db::model::WebhookDeliveryAttemptResult;
use nexus_db_queries::db::model::WebhookReceiverConfig;
use nexus_db_queries::db::pagination::Paginator;
use nexus_types::identity::Resource;
use nexus_types::internal_api::background::WebhookDeliveratorStatus;
use nexus_types::internal_api::background::WebhookDeliveryFailure;
use nexus_types::internal_api::background::WebhookRxDeliveryStatus;
use omicron_common::api::external::Error;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_uuid_kinds::{GenericUuid, OmicronZoneUuid, WebhookDeliveryUuid};
use parallel_task_set::ParallelTaskSet;
use std::sync::Arc;

// The Deliverator belongs to an elite order, a hallowed sub-category. He's got
// esprit up to here. Right now he is preparing to carry out his third mission
// of the night. His uniform is black as activated charcoal, filtering the very
// light out of the air. A bullet will bounce off its arachno-fiber weave like a
// wren hitting a patio door, but excess perspiration wafts through it like a
// breeze through a freshly napalmed forest. Where his body has bony
// extremities, the suit has sintered armorgel: feels like gritty jello,
// protects like a stack of telephone books.
//
// When they gave him the job, they gave him a gun. The Deliverator never deals
// in cash, but someone might come after him anyway–might want his car, or his
// cargo. The gun is tiny, aero-styled, lightweight, the kind of a gun a
// fashion designer would carry; it fires teensy darts that fly at five times
// the velocity of an SR-71 spy plane, and when you get done using it, you have
// to plug it in to the cigarette lighter, because it runs on electricity.
//
// The Deliverator never pulled that gun in anger, or in fear. He pulled it once
// in Gila Highlands. Some punks in Gila Highlands, a fancy Burbclave, wanted
// themselves a delivery, and they didn't want to pay for it. Thought they would
// impress the Deliverator with a baseball bat. The Deliverator took out his
// gun, centered its laser doo-hickey on that poised Louisville Slugger, fired
// it. The recoil was immense, as though the weapon had blown up in his hand.
// The middle third of the baseball bat turned into a column of burning sawdust
// accelerating in all directions like a bursting star. Punk ended up holding
// this bat handle with milky smoke pouring out the end. Stupid look on his
// face. Didn't get nothing but trouble from the Deliverator.
//
// Since then the Deliverator has kept the gun in the glove compartment and
// relied, instead, on a matched set of samurai swords, which have always been
// his weapon of choice anyhow. The punks in Gila Highlands weren't afraid of
// the gun, so the Deliverator was forced to use it. But swords need no
// demonstration.
//
// The Deliverator's car has enough potential energy packed into its batteries
// to fire a pound of bacon into the Asteroid Belt. Unlike a bimbo box or a Burb
// beater, the Deliverator's car unloads that power through gaping, gleaming,
// polished sphincters. When the Deliverator puts the hammer down, shit happens.
// You want to talk contact patches? Your car's tires have tiny contact patches,
// talk to the asphalt in four places the size of your tongue. The Deliverator's
// car has big sticky tires with contact patches the size of a fat lady's
// thighs. The Deliverator is in touch with the road, starts like a bad day,
// stops on a peseta.
//
// Why is the Deliverator so equipped? Because people rely on him. He is a role
// model.
//
// --- Neal Stephenson, _Snow Crash_
#[derive(Clone)]
pub struct WebhookDeliverator {
    datastore: Arc<DataStore>,
    nexus_id: OmicronZoneUuid,
    client: reqwest::Client,
    cfg: DeliveryConfig,
}

impl BackgroundTask for WebhookDeliverator {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        Box::pin(async move {
            let mut status = WebhookDeliveratorStatus {
                by_rx: Default::default(),
                error: None,
            };
            if let Err(e) = self.actually_activate(opctx, &mut status).await {
                slog::error!(
                    &opctx.log,
                    "webhook delivery failed";
                    "error" => %e,
                );
                status.error = Some(e.to_string());
            }

            serde_json::json!(status)
        })
    }
}

impl WebhookDeliverator {
    pub fn new(
        datastore: Arc<DataStore>,
        cfg: DeliveryConfig,
        nexus_id: OmicronZoneUuid,
        client: reqwest::Client,
    ) -> Self {
        Self { datastore, nexus_id, cfg, client }
    }

    const MAX_CONCURRENT_RXS: usize = 8;

    async fn actually_activate(
        &mut self,
        opctx: &OpContext,
        status: &mut WebhookDeliveratorStatus,
    ) -> Result<(), Error> {
        let mut tasks =
            ParallelTaskSet::new_with_parallelism(Self::MAX_CONCURRENT_RXS);
        let mut paginator =
            Paginator::new(nexus_db_queries::db::datastore::SQL_BATCH_SIZE);
        while let Some(p) = paginator.next() {
            let rxs = self
                .datastore
                .alert_rx_list(&opctx, &PaginatedBy::Id(p.current_pagparams()))
                .await?;
            paginator = p
                .found_batch(&rxs, &|WebhookReceiverConfig { rx, .. }| {
                    rx.id().into_untyped_uuid()
                });

            for rx in rxs {
                let rx_id = rx.rx.id();
                let opctx = opctx.child(maplit::btreemap! {
                    "rx_id".to_string() => rx_id.to_string(),
                    "rx_name".to_string() => rx.rx.name().to_string(),
                });
                let deliverator = self.clone();
                // Spawn the next delivery attempt, potentially joining a
                // previous one if the concurrency limit has been reached.
                let prev_result = tasks.spawn(async move {
                    let status = match deliverator.rx_deliver(&opctx, rx).await
                    {
                        Ok(status) => status,
                        Err(e) => {
                            slog::error!(
                                &opctx.log,
                                "failed to deliver webhook events to receiver";
                                "error" => %e,
                            );
                            WebhookRxDeliveryStatus {
                                error: Some(e.to_string()),
                                ..Default::default()
                            }
                        }
                    };
                    (rx_id, status)
                }).await;
                if let Some((rx_id, rx_status)) = prev_result {
                    status.by_rx.insert(rx_id, rx_status);
                }
            }
        }

        // Wait for the remaining batch of tasks to come back.
        while let Some((rx_id, rx_status)) = tasks.join_next().await {
            status.by_rx.insert(rx_id, rx_status);
        }

        slog::info!(
            &opctx.log,
            "all webhook delivery tasks completed";
            "num_receivers" => status.by_rx.len(),
        );

        Ok(())
    }

    async fn rx_deliver(
        &self,
        opctx: &OpContext,
        WebhookReceiverConfig { rx, secrets, .. }: WebhookReceiverConfig,
    ) -> Result<WebhookRxDeliveryStatus, anyhow::Error> {
        let mut client =
            ReceiverClient::new(&self.client, secrets, &rx, self.nexus_id)?;

        let deliveries = self
            .datastore
            .webhook_rx_delivery_list_ready(&opctx, &rx.id(), &self.cfg)
            .await
            .map_err(|e| {
                anyhow::anyhow!("could not list ready deliveries: {e}")
            })?;

        // Okay, we got everything we need in order to deliver events to this
        // receiver. Now, let's actually...do that.
        let mut delivery_status = WebhookRxDeliveryStatus {
            ready: deliveries.len(),
            ..Default::default()
        };

        for DeliveryAndEvent { delivery, alert_class, event } in deliveries {
            let attempt = (*delivery.attempts) + 1;
            let delivery_id = WebhookDeliveryUuid::from(delivery.id);
            match self
                .datastore
                .webhook_delivery_start_attempt(
                    opctx,
                    &delivery,
                    &self.nexus_id,
                    self.cfg.lease_timeout,
                )
                .await
            {
                Ok(DeliveryAttemptState::Started) => {
                    slog::trace!(&opctx.log,
                        "webhook event delivery attempt started";
                        "alert_id" => %delivery.alert_id,
                        "alert_class" => %alert_class,
                        "delivery_id" => %delivery_id,
                        "attempt" => ?attempt,
                    );
                }
                Ok(DeliveryAttemptState::AlreadyCompleted(time)) => {
                    slog::debug!(
                        &opctx.log,
                        "delivery of this webhook event was already completed \
                         at {time:?}";
                        "alert_id" => %delivery.alert_id,
                        "alert_class" => %alert_class,
                        "delivery_id" => %delivery_id,
                        "time_completed" => ?time,
                    );
                    delivery_status.already_delivered += 1;
                    continue;
                }
                Ok(DeliveryAttemptState::InProgress { nexus_id, started }) => {
                    slog::debug!(
                        &opctx.log,
                        "delivery of this webhook event is in progress by \
                         another Nexus";
                        "alert_id" => %delivery.alert_id,
                        "alert_class" => %alert_class,
                        "delivery_id" => %delivery_id,
                        "nexus_id" => %nexus_id,
                        "time_started" => ?started,
                    );
                    delivery_status.in_progress += 1;
                    continue;
                }
                Err(error) => {
                    slog::error!(
                        &opctx.log,
                        "unexpected database error error starting webhook \
                         delivery attempt";
                        "alert_id" => %delivery.alert_id,
                        "alert_class" => %alert_class,
                        "delivery_id" => %delivery_id,
                        "error" => %error,
                    );
                    delivery_status
                        .delivery_errors
                        .insert(delivery_id, error.to_string());
                    continue;
                }
            }

            // okay, actually do the thing...
            let delivery_attempt = match client
                .send_delivery_request(opctx, &delivery, alert_class, &event)
                .await
            {
                Ok(delivery) => delivery,
                Err(error) => {
                    delivery_status
                        .delivery_errors
                        .insert(delivery_id, format!("{error:?}"));
                    continue;
                }
            };

            if let Err(e) = self
                .datastore
                .webhook_delivery_finish_attempt(
                    opctx,
                    &delivery,
                    &self.nexus_id,
                    &delivery_attempt,
                )
                .await
            {
                const MSG: &str = "failed to mark webhook delivery as finished";
                slog::error!(
                    &opctx.log,
                    "{MSG}";
                    "alert_id" => %delivery.alert_id,
                    "alert_class" => %alert_class,
                    "delivery_id" => %delivery_id,
                    "error" => %e,
                );
                delivery_status
                    .delivery_errors
                    .insert(delivery_id, format!("{MSG}: {e}"));
            }

            if delivery_attempt.result
                == WebhookDeliveryAttemptResult::Succeeded
            {
                delivery_status.delivered_ok += 1;
            } else {
                delivery_status.failed_deliveries.push(
                    WebhookDeliveryFailure {
                        delivery_id,
                        alert_id: delivery.alert_id.into(),
                        attempt: delivery_attempt.attempt.0 as usize,
                        result: delivery_attempt.result.into(),
                        response_status: delivery_attempt
                            .response_status
                            .map(Into::into),
                        response_duration: delivery_attempt.response_duration,
                    },
                );
            }
        }

        Ok(delivery_status)
    }
}
