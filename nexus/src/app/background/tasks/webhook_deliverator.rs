// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
use crate::app::background::BackgroundTask;
use crate::app::webhook::ReceiverClient;
use futures::future::BoxFuture;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::datastore::webhook_delivery::DeliveryAttemptState;
pub use nexus_db_queries::db::datastore::webhook_delivery::DeliveryConfig;
use nexus_db_queries::db::model::WebhookDeliveryResult;
use nexus_db_queries::db::model::WebhookReceiverConfig;
use nexus_db_queries::db::pagination::Paginator;
use nexus_db_queries::db::DataStore;
use nexus_types::identity::Resource;
use nexus_types::internal_api::background::WebhookDeliveratorStatus;
use nexus_types::internal_api::background::WebhookDeliveryFailure;
use nexus_types::internal_api::background::WebhookRxDeliveryStatus;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::{GenericUuid, OmicronZoneUuid, WebhookDeliveryUuid};
use std::num::NonZeroU32;
use std::sync::Arc;
use tokio::task::JoinSet;

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
                slog::error!(&opctx.log, "webhook delivery failed"; "error" => %e);
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

    const MAX_CONCURRENT_RXS: NonZeroU32 = {
        match NonZeroU32::new(8) {
            Some(nz) => nz,
            None => unreachable!(),
        }
    };

    async fn actually_activate(
        &mut self,
        opctx: &OpContext,
        status: &mut WebhookDeliveratorStatus,
    ) -> Result<(), Error> {
        let mut tasks = JoinSet::new();
        let mut paginator = Paginator::new(Self::MAX_CONCURRENT_RXS);
        while let Some(p) = paginator.next() {
            let rxs = self
                .datastore
                .webhook_rx_list(
                    &opctx,
                    &PaginatedBy::Id(p.current_pagparams()),
                )
                .await?;
            paginator = p
                .found_batch(&rxs, &|WebhookReceiverConfig { rx, .. }| {
                    rx.id().into_untyped_uuid()
                });

            for rx in rxs {
                let rx_id = rx.rx.id();
                let opctx = opctx.child(maplit::btreemap! {
                    "receiver_id".to_string() => rx_id.to_string(),
                    "receiver_name".to_string() => rx.rx.name().to_string(),
                });
                let deliverator = self.clone();
                tasks.spawn(async move {
                    let status = match deliverator.rx_deliver(&opctx, rx).await {
                        Ok(status) => status,
                        Err(e) => {
                            slog::error!(
                                &opctx.log,
                                "failed to deliver webhook events to a receiver";
                                "rx_id" => ?rx_id,
                                "error" => %e,
                            );
                            WebhookRxDeliveryStatus {
                                error: Some(e.to_string()),
                                ..Default::default()
                            }
                        }
                    };
                    (rx_id, status)
                });
            }

            while let Some(result) = tasks.join_next().await {
                let (rx_id, rx_status) = result.expect(
                "delivery tasks should not be canceled, and nexus is compiled \
                with `panic=\"abort\"`, so they will not have panicked",
            );
                status.by_rx.insert(rx_id, rx_status);
            }
        }

        Ok(())
    }

    async fn rx_deliver(
        &self,
        opctx: &OpContext,
        WebhookReceiverConfig { rx, secrets, .. }: WebhookReceiverConfig,
    ) -> Result<WebhookRxDeliveryStatus, anyhow::Error> {
        let mut client = ReceiverClient::new(&self.client, secrets, &rx)?;

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

        for (delivery, event_class) in deliveries {
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
                        "event_id" => %delivery.event_id,
                        "event_class" => %event_class,
                        "delivery_id" => %delivery_id,
                        "attempt" => ?attempt,
                    );
                }
                Ok(DeliveryAttemptState::AlreadyCompleted(time)) => {
                    slog::debug!(
                        &opctx.log,
                        "delivery of this webhook event was already completed at {time:?}";
                        "event_id" => %delivery.event_id,
                        "event_class" => %event_class,
                        "delivery_id" => %delivery_id,
                        "time_completed" => ?time,
                    );
                    delivery_status.already_delivered += 1;
                    continue;
                }
                Ok(DeliveryAttemptState::InProgress { nexus_id, started }) => {
                    slog::debug!(
                        &opctx.log,
                        "delivery of this webhook event is in progress by another Nexus";
                        "event_id" => %delivery.event_id,
                        "event_class" => %event_class,
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
                        "unexpected database error error starting webhook delivery attempt";
                        "event_id" => %delivery.event_id,
                        "event_class" => %event_class,
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
                .send_delivery_request(opctx, &delivery, event_class)
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
                    "event_id" => %delivery.event_id,
                    "event_class" => %event_class,
                    "delivery_id" => %delivery_id,
                    "error" => %e,
                );
                delivery_status
                    .delivery_errors
                    .insert(delivery_id, format!("{MSG}: {e}"));
            }

            if delivery_attempt.result == WebhookDeliveryResult::Succeeded {
                delivery_status.delivered_ok += 1;
            } else {
                delivery_status.failed_deliveries.push(
                    WebhookDeliveryFailure {
                        delivery_id,
                        event_id: delivery.event_id.into(),
                        attempt: delivery_attempt.attempt.0 as usize,
                        result: delivery_attempt.result.into(),
                        response_status: delivery_attempt
                            .response_status
                            .map(|status| status as u16),
                        response_duration: delivery_attempt.response_duration,
                    },
                );
            }
        }

        Ok(delivery_status)
    }
}
