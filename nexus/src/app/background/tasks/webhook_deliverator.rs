// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
use crate::app::background::BackgroundTask;
use chrono::{TimeDelta, Utc};
use futures::future::BoxFuture;
use http::HeaderName;
use http::HeaderValue;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::datastore::webhook_delivery::DeliveryAttemptState;
use nexus_db_queries::db::model::{
    SqlU8, WebhookDeliveryAttempt, WebhookDeliveryResult, WebhookReceiver,
};
use nexus_db_queries::db::pagination::Paginator;
use nexus_db_queries::db::DataStore;
use nexus_types::identity::Resource;
use nexus_types::internal_api::background::{
    WebhookDeliveratorStatus, WebhookRxDeliveryStatus,
};
use omicron_common::api::external::Error;
use omicron_uuid_kinds::{
    GenericUuid, OmicronZoneUuid, WebhookDeliveryUuid, WebhookEventUuid,
    WebhookReceiverUuid,
};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{Duration, Instant};
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
    lease_timeout: TimeDelta,
    client: reqwest::Client,
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
        lease_timeout: TimeDelta,
        nexus_id: OmicronZoneUuid,
    ) -> Self {
        let client = reqwest::Client::builder()
            // Per [RFD 538 § 4.3.1][1], webhook delivery does *not* follow
            // redirects.
            //
            // [1]: https://rfd.shared.oxide.computer/rfd/538#_success
            .redirect(reqwest::redirect::Policy::none())
            // Per [RFD 538 § 4.3.2][1], the client must be able to connect to a
            // webhook receiver endpoint within 10 seconds, or the delivery is
            // considered failed.
            //
            // [1]: https://rfd.shared.oxide.computer/rfd/538#delivery-failure
            .connect_timeout(Duration::from_secs(10))
            .build()
            .expect("failed to configure webhook deliverator client!");
        Self { datastore, nexus_id, lease_timeout, client }
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
                .webhook_rx_list(&opctx, &p.current_pagparams())
                .await?;
            paginator = p.found_batch(&rxs, &|rx| rx.id().into_untyped_uuid());

            for rx in rxs {
                let opctx = opctx.child(maplit::btreemap! {
                    "receiver_id".to_string() => rx.id().to_string(),
                    "receiver_name".to_string() => rx.name().to_string(),
                });
                let deliverator = self.clone();
                tasks.spawn(async move {
                    let rx_id = rx.id();
                    let status = deliverator.rx_deliver(&opctx, rx).await;
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
        rx: WebhookReceiver,
    ) -> WebhookRxDeliveryStatus {
        let deliveries = match self
            .datastore
            .webhook_rx_delivery_list_ready(
                &opctx,
                &rx.id(),
                self.lease_timeout,
            )
            .await
        {
            Err(e) => {
                const MSG: &str = "failed to list ready deliveries";
                slog::error!(
                    &opctx.log,
                    "{MSG}";
                    "error" => %e,
                );
                return WebhookRxDeliveryStatus {
                    error: Some(format!("{MSG}: {e}")),
                    ..Default::default()
                };
            }
            Ok(deliveries) => deliveries,
        };
        let mut delivery_status = WebhookRxDeliveryStatus {
            ready: deliveries.len(),
            ..Default::default()
        };
        let hdr_rx_id = HeaderValue::try_from(rx.id().to_string())
            .expect("UUIDs should always be a valid header value");
        for (delivery, event_class) in deliveries {
            let attempt = (*delivery.attempts) + 1;
            let delivery_id = WebhookDeliveryUuid::from(delivery.id);
            let event_class = &event_class;
            match self
                .datastore
                .webhook_delivery_start_attempt(
                    opctx,
                    &delivery,
                    &self.nexus_id,
                    self.lease_timeout,
                )
                .await
            {
                Ok(DeliveryAttemptState::Started) => {
                    slog::trace!(&opctx.log,
                        "webhook event delivery attempt started";
                        "event_id" => %delivery.event_id,
                        "event_class" => event_class,
                        "delivery_id" => %delivery_id,
                        "attempt" => attempt,
                    );
                }
                Ok(DeliveryAttemptState::AlreadyCompleted(time)) => {
                    slog::debug!(
                        &opctx.log,
                        "delivery of this webhook event was already completed at {time:?}";
                        "event_id" => %delivery.event_id,
                        "event_class" => event_class,
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
                        "event_class" => event_class,
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
                        "event_class" => event_class,
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
            let time_attempted = Utc::now();
            let sent_at = time_attempted.to_rfc3339();
            let payload = Payload {
                event_class: event_class.as_ref(),
                event_id: delivery.event_id.into(),
                data: &delivery.payload,
                delivery: DeliveryMetadata {
                    id: delivery.id.into(),
                    webhook_id: rx.id(),
                    sent_at: &sent_at,
                },
            };
            // TODO(eliza): signatures!
            let request = self
                .client
                .post(&rx.endpoint)
                .header(HDR_RX_ID, hdr_rx_id.clone())
                .header(HDR_DELIVERY_ID, delivery_id.to_string())
                .header(HDR_EVENT_ID, delivery.event_id.to_string())
                .header(HDR_EVENT_CLASS, event_class)
                .json(&payload)
                // Per [RFD 538 § 4.3.2][1], a 30-second timeout is applied to
                // each webhook delivery request.
                //
                // [1]: https://rfd.shared.oxide.computer/rfd/538#delivery-failure
                .timeout(Duration::from_secs(30))
                .build();
            let request = match request {
                // We couldn't construct a request for some reason! This one's
                // our fault, so don't penalize the receiver for it.
                Err(e) => {
                    const MSG: &str = "failed to construct webhook request";
                    slog::error!(
                        &opctx.log,
                        "{MSG}";
                        "event_id" => %delivery.event_id,
                        "event_class" => event_class,
                        "delivery_id" => %delivery_id,
                        "error" => %e,
                        "payload" => ?payload,
                    );
                    delivery_status
                        .delivery_errors
                        .insert(delivery_id, format!("{MSG}: {e}"));
                    continue;
                }
                Ok(r) => r,
            };
            let t0 = Instant::now();
            let result = self.client.execute(request).await;
            let duration = t0.elapsed();
            let (delivery_result, status) = match result {
                // Builder errors are our fault, that's weird!
                Err(e) if e.is_builder() => {
                    const MSG: &str =
                        "internal error constructing webhook delivery request";
                    slog::error!(
                        &opctx.log,
                        "{MSG}";
                        "event_id" => %delivery.event_id,
                        "event_class" => event_class,
                        "delivery_id" => %delivery_id,
                        "error" => %e,
                    );
                    delivery_status
                        .delivery_errors
                        .insert(delivery_id, format!("{MSG}: {e}"));
                    continue;
                }
                Err(e) => {
                    if let Some(status) = e.status() {
                        slog::warn!(
                            &opctx.log,
                            "webhook receiver endpoint returned an HTTP error";
                            "event_id" => %delivery.event_id,
                            "event_class" => event_class,
                            "delivery_id" => %delivery_id,
                            "response_status" => ?status,
                            "response_duration" => ?duration,
                        );
                        (WebhookDeliveryResult::FailedHttpError, Some(status))
                    } else {
                        let result = if e.is_connect() {
                            WebhookDeliveryResult::FailedUnreachable
                        } else if e.is_timeout() {
                            WebhookDeliveryResult::FailedTimeout
                        } else if e.is_redirect() {
                            WebhookDeliveryResult::FailedHttpError
                        } else {
                            WebhookDeliveryResult::FailedUnreachable
                        };
                        slog::warn!(
                            &opctx.log,
                            "webhook delivery request failed";
                            "event_id" => %delivery.event_id,
                            "event_class" => event_class,
                            "delivery_id" => %delivery_id,
                            "error" => %e,
                        );
                        (result, None)
                    }
                }
                Ok(rsp) => {
                    let status = rsp.status();
                    slog::debug!(
                        &opctx.log,
                        "webhook event delivered successfully";
                        "event_id" => %delivery.event_id,
                        "event_class" => event_class,
                        "delivery_id" => %delivery_id,
                        "response_status" => ?status,
                        "response_duration" => ?duration,
                    );
                    (WebhookDeliveryResult::Succeeded, Some(status))
                }
            };
            // only include a response duration if we actually got a response back
            let response_duration = status.map(|_| {
                TimeDelta::from_std(duration).expect(
                    "because we set a 30-second response timeout, there is no \
                    way a response duration could ever exceed the max \
                    representable TimeDelta of `i64::MAX` milliseconds",
                )
            });
            let delivery_attempt = WebhookDeliveryAttempt {
                delivery_id: delivery.id,
                attempt: SqlU8::new(attempt),
                result: delivery_result,
                response_status: status.map(|s| s.as_u16() as i16),
                response_duration,
                time_created: chrono::Utc::now(),
            };

            match self
                .datastore
                .webhook_delivery_finish_attempt(
                    opctx,
                    &delivery,
                    &self.nexus_id,
                    &delivery_attempt,
                )
                .await
            {
                Err(e) => {
                    const MSG: &str =
                        "failed to mark webhook delivery as finished";
                    slog::error!(
                        &opctx.log,
                        "{MSG}";
                        "event_id" => %delivery.event_id,
                        "event_class" => event_class,
                        "delivery_id" => %delivery_id,
                        "error" => %e,
                    );
                    delivery_status
                        .delivery_errors
                        .insert(delivery_id, format!("{MSG}: {e}"));
                }
                Ok(_) => {
                    delivery_status.delivered_ok += 1;
                }
            }
        }

        // TODO(eliza): if no events were sent, do a probe...

        delivery_status
    }
}

const HDR_DELIVERY_ID: HeaderName =
    HeaderName::from_static("x-oxide-delivery-id");
const HDR_RX_ID: HeaderName = HeaderName::from_static("x-oxide-webhook-id");
const HDR_EVENT_ID: HeaderName = HeaderName::from_static("x-oxide-event-id");
const HDR_EVENT_CLASS: HeaderName =
    HeaderName::from_static("x-oxide-event-class");
// const HDR_SIG: HeaderName = HeaderName::from_static("x-oxide-signature");

#[derive(serde::Serialize, Debug)]
struct Payload<'a> {
    event_class: &'a str,
    event_id: WebhookEventUuid,
    data: &'a serde_json::Value,
    delivery: DeliveryMetadata<'a>,
}

#[derive(serde::Serialize, Debug)]
struct DeliveryMetadata<'a> {
    id: WebhookDeliveryUuid,
    webhook_id: WebhookReceiverUuid,
    sent_at: &'a str,
}
