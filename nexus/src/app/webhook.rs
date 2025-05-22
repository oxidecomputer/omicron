// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # Webhooks
//!
//! Webhooks provide a mechanism for delivering [alerts] by sending an HTTP
//! request representing the alert to an external HTTP server.
//!
//! [RFD 538] describes the user-facing interface for Oxide rack webhooks.
//! However, that RFD does not describe internal implementation details of the
//! webhook implementation, the key players, their roles, and interactions.
//! For details on the internals of alert delivery, including webhooks, see the
//! documenation in the [`alert` module][alerts].
//!
//! Generic operations on all types of alert receivers, such as listing
//! receivers and adding/removing subscriptions, are defined in the
//! [`alert` module][alerts].  Operations relating to webhook-specific
//! configurations or concepts, such as managing secrets, sending liveness
//! probes, and creating and updating webhook receiver configuration, are
//! defined here.  This module also defines [`ReceiverClient`], which
//! implements the HTTP client for sending webhook requests to webhook
//! receivers.  The client implementation is defined here, as it is used by
//! both the API (for probe requests) and the `webhook_deliverator` background
//! task, which performs the delivery of queued alerts.
//!
//! [alerts]: super::alert
//! [RFD 538]: https://rfd.shared.oxide.computer/538

use crate::Nexus;
use anyhow::Context;
use chrono::TimeDelta;
use chrono::Utc;
use hmac::{Hmac, Mac};
use http::HeaderName;
use http::HeaderValue;
use nexus_db_lookup::LookupPath;
use nexus_db_lookup::lookup;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::model::AlertClass;
use nexus_db_queries::db::model::AlertDeliveryState;
use nexus_db_queries::db::model::AlertDeliveryTrigger;
use nexus_db_queries::db::model::AlertReceiver;
use nexus_db_queries::db::model::SqlU8;
use nexus_db_queries::db::model::WebhookDelivery;
use nexus_db_queries::db::model::WebhookDeliveryAttempt;
use nexus_db_queries::db::model::WebhookDeliveryAttemptResult;
use nexus_db_queries::db::model::WebhookReceiverConfig;
use nexus_db_queries::db::model::WebhookSecret;
use nexus_types::external_api::params;
use nexus_types::external_api::views;
use nexus_types::identity::Asset;
use nexus_types::identity::Resource;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::UpdateResult;
use omicron_uuid_kinds::AlertReceiverUuid;
use omicron_uuid_kinds::AlertUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::WebhookDeliveryAttemptUuid;
use omicron_uuid_kinds::WebhookDeliveryUuid;
use omicron_uuid_kinds::WebhookSecretUuid;
use sha2::Sha256;
use std::sync::LazyLock;
use std::time::Duration;
use std::time::Instant;

impl Nexus {
    pub fn webhook_secret_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        secret_selector: params::WebhookSecretSelector,
    ) -> LookupResult<lookup::WebhookSecret<'a>> {
        let lookup = LookupPath::new(&opctx, self.datastore())
            .webhook_secret_id(WebhookSecretUuid::from_untyped_uuid(
                secret_selector.secret_id,
            ));
        Ok(lookup)
    }

    pub async fn webhook_receiver_create(
        &self,
        opctx: &OpContext,
        params: params::WebhookCreate,
    ) -> CreateResult<WebhookReceiverConfig> {
        self.datastore().webhook_rx_create(&opctx, params).await
    }

    pub async fn webhook_receiver_update(
        &self,
        opctx: &OpContext,
        rx: lookup::AlertReceiver<'_>,
        params: params::WebhookReceiverUpdate,
    ) -> UpdateResult<()> {
        let (authz_rx,) = rx.lookup_for(authz::Action::Modify).await?;
        let _ = self
            .datastore()
            .webhook_rx_update(opctx, &authz_rx, params)
            .await?;
        Ok(())
    }

    pub async fn webhook_receiver_delete(
        &self,
        opctx: &OpContext,
        rx: lookup::AlertReceiver<'_>,
    ) -> DeleteResult {
        let (authz_rx, db_rx) = rx.fetch_for(authz::Action::Delete).await?;
        self.datastore().webhook_rx_delete(&opctx, &authz_rx, &db_rx).await
    }

    //
    // Receiver secret API methods
    //

    pub async fn webhook_receiver_secrets_list(
        &self,
        opctx: &OpContext,
        rx: lookup::AlertReceiver<'_>,
    ) -> ListResultVec<WebhookSecret> {
        let (authz_rx,) = rx.lookup_for(authz::Action::ListChildren).await?;
        self.datastore().webhook_rx_secret_list(opctx, &authz_rx).await
    }

    pub async fn webhook_receiver_secret_add(
        &self,
        opctx: &OpContext,
        rx: lookup::AlertReceiver<'_>,
        secret: String,
    ) -> Result<views::WebhookSecret, Error> {
        let (authz_rx,) = rx.lookup_for(authz::Action::CreateChild).await?;
        let secret = WebhookSecret::new(authz_rx.id(), secret);
        let secret = self
            .datastore()
            .webhook_rx_secret_create(opctx, &authz_rx, secret)
            .await?;
        slog::info!(
            &opctx.log,
            "added secret to webhook receiver";
            "rx_id" => ?authz_rx.id(),
            "secret_id" => ?secret.identity.id,
            "time_created"=> %secret.identity.time_created,
        );
        Ok(secret.into())
    }

    pub async fn webhook_receiver_secret_delete(
        &self,
        opctx: &OpContext,
        secret: lookup::WebhookSecret<'_>,
    ) -> DeleteResult {
        let (authz_rx, authz_secret) =
            secret.lookup_for(authz::Action::Delete).await?;
        self.datastore()
            .webhook_rx_secret_delete(&opctx, &authz_rx, &authz_secret)
            .await?;
        slog::info!(
            &opctx.log,
            "deleted secret from webhook receiver";
            "rx_id" => ?authz_rx.id(),
            "secret_id" => ?authz_secret.id(),
        );
        Ok(())
    }

    //
    // Receiver event delivery API methods
    //

    pub async fn webhook_receiver_probe(
        &self,
        opctx: &OpContext,
        rx: lookup::AlertReceiver<'_>,
        params: params::AlertReceiverProbe,
    ) -> Result<views::AlertProbeResult, Error> {
        let (authz_rx, rx) = rx.fetch_for(authz::Action::ListChildren).await?;
        let rx_id = authz_rx.id();
        let datastore = self.datastore();
        let secrets =
            datastore.webhook_rx_secret_list(opctx, &authz_rx).await?;
        let mut client = ReceiverClient::new(
            &self.webhook_delivery_client,
            secrets,
            &rx,
            self.id,
        )?;
        let mut delivery = WebhookDelivery::new_probe(&rx_id, &self.id);

        const CLASS: AlertClass = AlertClass::Probe;
        static DATA: LazyLock<serde_json::Value> =
            LazyLock::new(|| serde_json::json!({}));

        let attempt = match client
            .send_delivery_request(opctx, &delivery, CLASS, &DATA)
            .await
        {
            Ok(attempt) => attempt,
            Err(e) => {
                slog::error!(
                    &opctx.log,
                    "failed to probe webhook receiver";
                    "rx_id" => %authz_rx.id(),
                    "rx_name" => %rx.name(),
                    "delivery_id" => %delivery.id,
                    "error" => %e,
                );
                return Err(Error::InternalError {
                    internal_message: e.to_string(),
                });
            }
        };

        // Update the delivery state based on the result of the probe attempt.
        // Otherwise, it will still appear "pending", which is obviously wrong.
        delivery.state = if attempt.result.is_failed() {
            AlertDeliveryState::Failed
        } else {
            AlertDeliveryState::Delivered
        };

        let resends_started = if params.resend
            && attempt.result == WebhookDeliveryAttemptResult::Succeeded
        {
            slog::debug!(
                &opctx.log,
                "webhook liveness probe succeeded, resending failed \
                 deliveries...";
                "rx_id" => %authz_rx.id(),
                "rx_name" => %rx.name(),
                "delivery_id" => %delivery.id,
            );

            let deliveries = datastore
                .webhook_rx_list_resendable_events(opctx, &rx_id)
                .await
                .map_err(|e| {
                    e.internal_context("error listing events to resend")
                })?
                .into_iter()
                .map(|event| {
                    slog::trace!(
                        &opctx.log,
                        "will resend webhook event after probe success";
                        "rx_id" => ?authz_rx.id(),
                        "rx_name" => %rx.name(),
                        "delivery_id" => ?delivery.id,
                        "alert_id" => ?event.id(),
                        "alert_class" => %event.class,
                    );
                    WebhookDelivery::new(
                        &event.id(),
                        &rx_id,
                        AlertDeliveryTrigger::Resend,
                    )
                })
                .collect::<Vec<_>>();
            let events_found = deliveries.len();

            let started = datastore
                .webhook_delivery_create_batch(&opctx, deliveries)
                .await
                .map_err(|e| {
                    e.internal_context(
                        "error creating deliveries to resend failed events",
                    )
                })?;

            if started > 0 {
                slog::info!(
                    &opctx.log,
                    "webhook liveness probe succeeded, created {started} \
                     re-deliveries";
                    "rx_id" => %authz_rx.id(),
                    "rx_name" => %rx.name(),
                    "delivery_id" => %delivery.id,
                    "events_found" => events_found,
                    "deliveries_started" => started,
                );
                // If new deliveries were created, activate the webhook
                // deliverator background task to start actually delivering
                // them.
                self.background_tasks.task_webhook_deliverator.activate();
            } else {
                slog::debug!(
                    &opctx.log,
                    "webhook liveness probe succeeded, but no failed events \
                     were re-delivered";
                    "rx_id" => %authz_rx.id(),
                    "rx_name" => %rx.name(),
                    "delivery_id" => %delivery.id,
                    "events_found" => events_found,
                );
            }

            Some(started)
        } else {
            None
        };

        Ok(views::AlertProbeResult {
            probe: delivery.to_api_delivery(CLASS, &[attempt]),
            resends_started,
        })
    }
}

/// Construct a [`reqwest::Client`] configured for webhook delivery requests.
pub(super) fn delivery_client(
    builder: reqwest::ClientBuilder,
) -> Result<reqwest::Client, reqwest::Error> {
    builder
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
        // Per [RFD 538 § 4.3.2][1], a 30-second timeout is applied to
        // each webhook delivery request.
        //
        // [1]: https://rfd.shared.oxide.computer/rfd/538#delivery-failure
        .timeout(Duration::from_secs(30))
        .build()
}

/// Everything necessary to send a delivery request to a webhook receiver.
///
/// This is its' own thing, rather than part of the `webhook_deliverator`
/// background task, as it is used both by the deliverator RPW and by the Nexus
/// API in the liveness probe endpoint.
pub(crate) struct ReceiverClient<'a> {
    client: &'a reqwest::Client,
    rx: &'a AlertReceiver,
    secrets: Vec<(WebhookSecretUuid, Hmac<Sha256>)>,
    hdr_rx_id: http::HeaderValue,
    nexus_id: OmicronZoneUuid,
}

impl<'a> ReceiverClient<'a> {
    pub(crate) fn new(
        client: &'a reqwest::Client,
        secrets: impl IntoIterator<Item = WebhookSecret>,
        rx: &'a AlertReceiver,
        nexus_id: OmicronZoneUuid,
    ) -> Result<Self, Error> {
        let secrets = secrets
            .into_iter()
            .map(|WebhookSecret { identity, secret, .. }| {
                let mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
                    .expect("HMAC key can be any size; this should never fail");
                (identity.id.into(), mac)
            })
            .collect::<Vec<_>>();
        if secrets.is_empty() {
            return Err(Error::invalid_request(
                "receiver has no secrets, so delivery requests cannot be sent",
            ));
        }
        let hdr_rx_id = HeaderValue::try_from(rx.id().to_string())
            .expect("UUIDs should always be a valid header value");
        Ok(Self { client, secrets, hdr_rx_id, rx, nexus_id })
    }

    pub(crate) async fn send_delivery_request(
        &mut self,
        opctx: &OpContext,
        delivery: &WebhookDelivery,
        alert_class: AlertClass,
        data: &serde_json::Value,
    ) -> Result<WebhookDeliveryAttempt, anyhow::Error> {
        const HDR_DELIVERY_ID: HeaderName =
            HeaderName::from_static("x-oxide-delivery-id");
        const HDR_RX_ID: HeaderName =
            HeaderName::from_static("x-oxide-receiver-id");
        const HDR_ALERT_ID: HeaderName =
            HeaderName::from_static("x-oxide-alert-id");
        const HDR_ALERT_CLASS: HeaderName =
            HeaderName::from_static("x-oxide-alert-class");
        const HDR_SIG: HeaderName =
            HeaderName::from_static("x-oxide-signature");
        const HDR_TIMESTAMP: HeaderName =
            HeaderName::from_static("x-oxide-timestamp");

        #[derive(serde::Serialize, Debug)]
        struct Payload<'a> {
            alert_class: AlertClass,
            alert_id: AlertUuid,
            data: &'a serde_json::Value,
            delivery: DeliveryMetadata<'a>,
        }

        #[derive(serde::Serialize, Debug)]
        struct DeliveryMetadata<'a> {
            id: WebhookDeliveryUuid,
            receiver_id: AlertReceiverUuid,
            sent_at: &'a str,
            trigger: views::AlertDeliveryTrigger,
        }

        // okay, actually do the thing...
        let time_attempted = Utc::now();
        let sent_at = time_attempted.to_rfc3339();
        let payload = Payload {
            alert_class,
            alert_id: delivery.alert_id.into(),
            data,
            delivery: DeliveryMetadata {
                id: delivery.id.into(),
                receiver_id: self.rx.id(),
                sent_at: &sent_at,
                trigger: delivery.triggered_by.into(),
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
                    "alert payload could not be serialized";
                slog::error!(
                    &opctx.log,
                    "webhook {MSG}";
                    "alert_id" => %delivery.alert_id,
                    "alert_class" => %alert_class,
                    "delivery_id" => %delivery.id,
                    "delivery_trigger" => %delivery.triggered_by,
                    "error" => %e,
                );

                // This really shouldn't happen --- we expect the alert
                // payload will always be valid JSON. We could *probably*
                // just panic here unconditionally, but it seems nicer to
                // try and do the other alerts. But, if there's ever a bug
                // that breaks serialization for *all* webhook payloads,
                // I'd like the tests to fail in a more obvious way than
                // eventually timing out waiting for the event to be
                // delivered ...
                if cfg!(debug_assertions) {
                    panic!("{MSG}: {e}\npayload: {payload:#?}");
                }
                return Err(e).context(MSG);
            }
        };
        let mut request = self
            .client
            .post(&self.rx.endpoint)
            .header(HDR_RX_ID, self.hdr_rx_id.clone())
            .header(HDR_DELIVERY_ID, delivery.id.to_string())
            .header(HDR_ALERT_ID, delivery.alert_id.to_string())
            .header(HDR_ALERT_CLASS, alert_class.to_string())
            .header(HDR_TIMESTAMP, sent_at)
            .header(http::header::CONTENT_TYPE, "application/json");

        // For each secret assigned to this webhook, calculate the HMAC and add a signature header.
        for (secret_id, mac) in &mut self.secrets {
            mac.update(&body);
            let sig_bytes = mac.finalize_reset().into_bytes();
            let sig = hex::encode(&sig_bytes[..]);
            request = request
                .header(HDR_SIG, format!("a=sha256&id={secret_id}&s={sig}"));
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
                    "alert_id" => %delivery.alert_id,
                    "alert_class" => %alert_class,
                    "delivery_id" => %delivery.id,
                    "delivery_trigger" => %delivery.triggered_by,
                    "error" => %e,
                    "payload" => ?payload,
                );
                return Err(e).context(MSG);
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
                    "alert_id" => %delivery.alert_id,
                    "alert_class" => %alert_class,
                    "delivery_id" => %delivery.id,
                    "delivery_trigger" => %delivery.triggered_by,
                    "error" => %e,
                );
                return Err(e).context(MSG);
            }
            Err(e) => {
                if let Some(status) = e.status() {
                    slog::warn!(
                        &opctx.log,
                        "webhook receiver endpoint returned an HTTP error";
                        "alert_id" => %delivery.alert_id,
                        "alert_class" => %alert_class,
                        "delivery_id" => %delivery.id,
                        "delivery_trigger" => %delivery.triggered_by,
                        "response_status" => ?status,
                        "response_duration" => ?duration,
                    );
                    (
                        WebhookDeliveryAttemptResult::FailedHttpError,
                        Some(status),
                    )
                } else {
                    let result = if e.is_connect() {
                        WebhookDeliveryAttemptResult::FailedUnreachable
                    } else if e.is_timeout() {
                        WebhookDeliveryAttemptResult::FailedTimeout
                    } else if e.is_redirect() {
                        WebhookDeliveryAttemptResult::FailedHttpError
                    } else {
                        WebhookDeliveryAttemptResult::FailedUnreachable
                    };
                    slog::warn!(
                        &opctx.log,
                        "webhook delivery request failed";
                        "alert_id" => %delivery.alert_id,
                        "alert_class" => %alert_class,
                        "delivery_id" => %delivery.id,
                        "delivery_trigger" => %delivery.triggered_by,
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
                        "alert_id" => %delivery.alert_id,
                        "alert_class" => %alert_class,
                        "delivery_id" => %delivery.id,
                        "delivery_trigger" => %delivery.triggered_by,
                        "response_status" => ?status,
                        "response_duration" => ?duration,
                    );
                    (WebhookDeliveryAttemptResult::Succeeded, Some(status))
                } else {
                    slog::warn!(
                        &opctx.log,
                        "webhook receiver endpoint returned an HTTP error";
                        "alert_id" => %delivery.alert_id,
                        "alert_class" => %alert_class,
                        "delivery_id" => %delivery.id,
                        "delivery_trigger" => %delivery.triggered_by,
                        "response_status" => ?status,
                        "response_duration" => ?duration,
                    );
                    (
                        WebhookDeliveryAttemptResult::FailedHttpError,
                        Some(status),
                    )
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

        Ok(WebhookDeliveryAttempt {
            id: WebhookDeliveryAttemptUuid::new_v4().into(),
            delivery_id: delivery.id,
            rx_id: delivery.rx_id,
            attempt: SqlU8::new(delivery.attempts.0 + 1),
            result: delivery_result,
            response_status: status.map(|s| s.as_u16().into()),
            response_duration,
            time_created: chrono::Utc::now(),
            deliverator_id: self.nexus_id.into(),
        })
    }
}
