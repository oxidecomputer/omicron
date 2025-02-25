// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
use crate::app::authz;
use crate::app::background::BackgroundTask;
use crate::app::db::lookup::LookupPath;
use chrono::{TimeDelta, Utc};
use futures::future::BoxFuture;
use hmac::{Hmac, Mac};
use http::HeaderName;
use http::HeaderValue;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::datastore::webhook_delivery::DeliveryAttemptState;
pub use nexus_db_queries::db::datastore::webhook_delivery::DeliveryConfig;
use nexus_db_queries::db::model::{
    SqlU8, WebhookDeliveryAttempt, WebhookDeliveryResult, WebhookEventClass,
    WebhookReceiver, WebhookSecret,
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
use sha2::Sha256;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Instant;
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
        rx: WebhookReceiver,
    ) -> Result<WebhookRxDeliveryStatus, anyhow::Error> {
        // First, look up the receiver's secrets and any deliveries for that
        // receiver. If any of these lookups fail, bail out, as we can't
        // meaningfully deliver any events to a receiver if we don't know what
        // they are or how to sign them.
        let (authz_rx,) = LookupPath::new(opctx, &self.datastore)
            .webhook_receiver_id(rx.id())
            .lookup_for(authz::Action::ListChildren)
            .await
            .map_err(|e| anyhow::anyhow!("could not look up receiver: {e}"))?;
        let mut secrets = self
            .datastore
            .webhook_rx_secret_list(opctx, &authz_rx)
            .await
            .map_err(|e| anyhow::anyhow!("could not list secrets: {e}"))?
            .into_iter()
            .map(|WebhookSecret { identity, secret, .. }| {
                let mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
                    .expect("HMAC key can be any size; this should never fail");
                (identity.id, mac)
            })
            .collect::<Vec<_>>();

        anyhow::ensure!(!secrets.is_empty(), "receiver has no secrets");
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
        let hdr_rx_id = HeaderValue::try_from(rx.id().to_string())
            .expect("UUIDs should always be a valid header value");
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
            let time_attempted = Utc::now();
            let sent_at = time_attempted.to_rfc3339();
            let payload = Payload {
                event_class,
                event_id: delivery.event_id.into(),
                data: &delivery.payload,
                delivery: DeliveryMetadata {
                    id: delivery.id.into(),
                    webhook_id: rx.id(),
                    sent_at: &sent_at,
                },
            };
            // N.B. that we serialize the body "ourselves" rather than just
            // passing it to `RequestBuilder::json` because we must access
            // the serialized body in order to calculate HMAC signatures.
            // This means we have to add the `Content-Type` ourselves below.
            let body = match serde_json::to_vec(&payload) {
                Ok(body) => body,
                Err(e) => {
                    const MSG: &'static str =
                        "event payload could not be serialized";
                    slog::error!(
                        &opctx.log,
                        "webhook {MSG}";
                        "event_id" => %delivery.event_id,
                        "event_class" => %event_class,
                        "delivery_id" => %delivery_id,
                        "error" => %e,
                    );

                    // This really shouldn't happen --- we expect the event
                    // payload will always be valid JSON. We could *probably*
                    // just panic here unconditionally, but it seems nicer to
                    // try and do the other events. But, if there's ever a bug
                    // that breaks serialization for *all* webhook payloads,
                    // I'd like the tests to fail in a more obvious way than
                    // eventually timing out waiting for the event to be
                    // delivered ...
                    if cfg!(debug_assertions) {
                        panic!("{MSG}:{e}\npayload: {payload:#?}");
                    }
                    delivery_status
                        .delivery_errors
                        .insert(delivery_id, format!("{MSG}: {e}"));
                    continue;
                }
            };
            let mut request = self
                .client
                .post(&rx.endpoint)
                .header(HDR_RX_ID, hdr_rx_id.clone())
                .header(HDR_DELIVERY_ID, delivery_id.to_string())
                .header(HDR_EVENT_ID, delivery.event_id.to_string())
                .header(HDR_EVENT_CLASS, event_class.to_string())
                .header(http::header::CONTENT_TYPE, "application/json");

            // For each secret assigned to this webhook, calculate the HMAC and add a signature header.
            for (secret_id, mac) in &mut secrets {
                mac.update(&body);
                let sig_bytes = mac.finalize_reset().into_bytes();
                let sig = hex::encode(&sig_bytes[..]);
                request = request.header(
                    HDR_SIG,
                    format!("a=sha256&id={secret_id}&s={sig}"),
                );
            }
            let request = request.body(body).build();

            let request = match request {
                // We couldn't construct a request for some reason! This one's
                // our fault, so don't penalize the receiver for it.
                Err(e) => {
                    const MSG: &str = "failed to construct webhook request";
                    slog::error!(
                        &opctx.log,
                        "{MSG}";
                        "event_id" => %delivery.event_id,
                        "event_class" => %event_class,
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
                        "event_class" => %event_class,
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
                            "event_class" => %event_class,
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
                            "event_class" => %event_class,
                            "delivery_id" => %delivery_id,
                            "error" => %e,
                        );
                        (result, None)
                    }
                }
                Ok(rsp) => {
                    let status = rsp.status();
                    if status.is_success() {
                        slog::debug!(
                            &opctx.log,
                            "webhook event delivered successfully";
                            "event_id" => %delivery.event_id,
                            "event_class" => %event_class,
                            "delivery_id" => %delivery_id,
                            "response_status" => ?status,
                            "response_duration" => ?duration,
                        );
                        (WebhookDeliveryResult::Succeeded, Some(status))
                    } else {
                        slog::warn!(
                            &opctx.log,
                            "webhook receiver endpoint returned an HTTP error";
                            "event_id" => %delivery.event_id,
                            "event_class" => %event_class,
                            "delivery_id" => %delivery_id,
                            "response_status" => ?status,
                            "response_duration" => ?duration,
                        );
                        (WebhookDeliveryResult::FailedHttpError, Some(status))
                    }
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
                        "event_class" => %event_class,
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

        Ok(delivery_status)
    }
}

const HDR_DELIVERY_ID: HeaderName =
    HeaderName::from_static("x-oxide-delivery-id");
const HDR_RX_ID: HeaderName = HeaderName::from_static("x-oxide-webhook-id");
const HDR_EVENT_ID: HeaderName = HeaderName::from_static("x-oxide-event-id");
const HDR_EVENT_CLASS: HeaderName =
    HeaderName::from_static("x-oxide-event-class");
const HDR_SIG: HeaderName = HeaderName::from_static("x-oxide-signature");

#[derive(serde::Serialize, Debug)]
struct Payload<'a> {
    event_class: WebhookEventClass,
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
