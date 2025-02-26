// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Webhooks

use crate::app::external_dns;
use anyhow::Context;
use chrono::TimeDelta;
use chrono::Utc;
use hmac::{Hmac, Mac};
use http::HeaderName;
use http::HeaderValue;
use nexus_db_model::WebhookReceiver;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::lookup;
use nexus_db_queries::db::lookup::LookupPath;
use nexus_db_queries::db::model::SqlU8;
use nexus_db_queries::db::model::WebhookDelivery;
use nexus_db_queries::db::model::WebhookDeliveryAttempt;
use nexus_db_queries::db::model::WebhookDeliveryResult;
use nexus_db_queries::db::model::WebhookEvent;
use nexus_db_queries::db::model::WebhookEventClass;
use nexus_db_queries::db::model::WebhookReceiverConfig;
use nexus_db_queries::db::model::WebhookSecret;
use nexus_types::external_api::params;
use nexus_types::external_api::views;
use nexus_types::identity::Resource;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::NameOrId;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::WebhookDeliveryUuid;
use omicron_uuid_kinds::WebhookEventUuid;
use omicron_uuid_kinds::WebhookReceiverUuid;
use omicron_uuid_kinds::WebhookSecretUuid;
use sha2::Sha256;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

impl super::Nexus {
    pub fn webhook_receiver_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        webhook_selector: params::WebhookSelector,
    ) -> LookupResult<lookup::WebhookReceiver<'a>> {
        match webhook_selector.webhook {
            NameOrId::Id(id) => {
                let webhook = LookupPath::new(opctx, &self.db_datastore)
                    .webhook_receiver_id(
                        WebhookReceiverUuid::from_untyped_uuid(id),
                    );
                Ok(webhook)
            }
            NameOrId::Name(name) => {
                let webhook = LookupPath::new(opctx, &self.db_datastore)
                    .webhook_receiver_name_owned(name.into());
                Ok(webhook)
            }
        }
    }

    pub async fn webhook_receiver_config_fetch(
        &self,
        opctx: &OpContext,
        rx: lookup::WebhookReceiver<'_>,
    ) -> LookupResult<WebhookReceiverConfig> {
        let (authz_rx, rx) = rx.fetch().await?;
        let (events, secrets) =
            self.datastore().webhook_rx_config_fetch(opctx, &authz_rx).await?;
        Ok(WebhookReceiverConfig { rx, secrets, events })
    }

    pub async fn webhook_receiver_create(
        &self,
        opctx: &OpContext,
        params: params::WebhookCreate,
    ) -> CreateResult<WebhookReceiverConfig> {
        // TODO(eliza): validate endpoint URI; reject underlay network IPs for
        // SSRF prevention...
        self.datastore().webhook_rx_create(&opctx, params).await
    }

    pub async fn webhook_event_publish(
        &self,
        opctx: &OpContext,
        id: WebhookEventUuid,
        event_class: WebhookEventClass,
        event: serde_json::Value,
    ) -> Result<WebhookEvent, Error> {
        let event = self
            .datastore()
            .webhook_event_create(opctx, id, event_class, event)
            .await?;
        slog::debug!(
            &opctx.log,
            "enqueued webhook event";
            "event_id" => ?id,
            "event_class" => %event.event_class,
            "time_created" => ?event.time_created,
        );

        // Once the event has been isnerted, activate the dispatcher task to
        // ensure its propagated to receivers.
        self.background_tasks.task_webhook_dispatcher.activate();

        Ok(event)
    }

    pub async fn webhook_receiver_probe(
        &self,
        opctx: &OpContext,
        rx: lookup::WebhookReceiver<'_>,
        _params: params::WebhookProbe,
    ) -> Result<views::WebhookDelivery, Error> {
        let (authz_rx, rx) = rx.fetch_for(authz::Action::ListChildren).await?;
        let secrets =
            self.datastore().webhook_rx_secret_list(opctx, &authz_rx).await?;
        let mut client =
            ReceiverClient::new(&self.webhook_delivery_client, secrets, &rx)?;
        let delivery = WebhookDelivery::new_probe(&authz_rx.id(), &self.id);

        const CLASS: WebhookEventClass = WebhookEventClass::Probe;

        let attempt =
            match client.send_delivery_request(opctx, &delivery, CLASS).await {
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

        // TODO(eliza): this is where we would resend all the failed stuff
        // if requested...

        Ok(delivery.to_api_delivery(CLASS, Some(&attempt)))
    }

    pub async fn webhook_receiver_secret_add(
        &self,
        opctx: &OpContext,
        rx: lookup::WebhookReceiver<'_>,
        secret: String,
    ) -> Result<views::WebhookSecretId, Error> {
        let (authz_rx,) = rx.lookup_for(authz::Action::CreateChild).await?;
        let secret = WebhookSecret::new(authz_rx.id(), secret);
        let WebhookSecret { identity, .. } = self
            .datastore()
            .webhook_rx_secret_create(opctx, &authz_rx, secret)
            .await?;
        let secret_id = identity.id;
        slog::info!(
            &opctx.log,
            "added secret to webhook receiver";
            "rx_id" => ?authz_rx.id(),
            "secret_id" => ?secret_id,
        );
        Ok(views::WebhookSecretId { id: secret_id.into_untyped_uuid() })
    }
}

/// Construct a [`reqwest::Client`] configured for webhook delivery requests.
pub(super) fn delivery_client(
    external_dns: &Arc<external_dns::Resolver>,
) -> Result<reqwest::Client, reqwest::Error> {
    /// A wrapper around [`external_dns::Resolver`] which rejects IP addresses that
    /// are underlay network IPs.
    struct WebhookDnsResolver {
        external_dns: Arc<external_dns::Resolver>,
    }

    impl reqwest::dns::Resolve for WebhookDnsResolver {
        fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
            // TODO(eliza): this is where we have to actually return an error if the
            // DNS name resolves to an underlay IP! Figure that out!
            self.external_dns.resolve(name)
        }
    }

    reqwest::Client::builder()
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
        // my god...it's full of Arcs...
        .dns_resolver(Arc::new(WebhookDnsResolver {
            external_dns: external_dns.clone(),
        }))
        .build()
}

pub(crate) struct ReceiverClient<'a> {
    client: &'a reqwest::Client,
    rx: &'a WebhookReceiver,
    secrets: Vec<(WebhookSecretUuid, Hmac<Sha256>)>,
    hdr_rx_id: http::HeaderValue,
}

impl<'a> ReceiverClient<'a> {
    pub(crate) fn new(
        client: &'a reqwest::Client,
        secrets: impl IntoIterator<Item = WebhookSecret>,
        rx: &'a WebhookReceiver,
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
        Ok(Self { client, secrets, hdr_rx_id, rx })
    }

    pub(crate) async fn send_delivery_request(
        &mut self,
        opctx: &OpContext,
        delivery: &WebhookDelivery,
        event_class: WebhookEventClass,
    ) -> Result<WebhookDeliveryAttempt, anyhow::Error> {
        const HDR_DELIVERY_ID: HeaderName =
            HeaderName::from_static("x-oxide-delivery-id");
        const HDR_RX_ID: HeaderName =
            HeaderName::from_static("x-oxide-webhook-id");
        const HDR_EVENT_ID: HeaderName =
            HeaderName::from_static("x-oxide-event-id");
        const HDR_EVENT_CLASS: HeaderName =
            HeaderName::from_static("x-oxide-event-class");
        const HDR_SIG: HeaderName =
            HeaderName::from_static("x-oxide-signature");

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
            trigger: views::WebhookDeliveryTrigger,
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
                webhook_id: self.rx.id(),
                sent_at: &sent_at,
                trigger: delivery.trigger.into(),
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
                    "delivery_id" => %delivery.id,
                    "delivery_trigger" => %delivery.trigger,
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
            .header(HDR_EVENT_ID, delivery.event_id.to_string())
            .header(HDR_EVENT_CLASS, event_class.to_string())
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
                    "event_id" => %delivery.event_id,
                    "event_class" => %event_class,
                    "delivery_id" => %delivery.id,
                    "delivery_trigger" => %delivery.trigger,
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
                    "event_id" => %delivery.event_id,
                    "event_class" => %event_class,
                    "delivery_id" => %delivery.id,
                    "delivery_trigger" => %delivery.trigger,
                    "error" => %e,
                );
                return Err(e).context(MSG);
            }
            Err(e) => {
                if let Some(status) = e.status() {
                    slog::warn!(
                        &opctx.log,
                        "webhook receiver endpoint returned an HTTP error";
                        "event_id" => %delivery.event_id,
                        "event_class" => %event_class,
                        "delivery_id" => %delivery.id,
                        "delivery_trigger" => %delivery.trigger,
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
                        "delivery_id" => %delivery.id,
                        "delivery_trigger" => %delivery.trigger,
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
                        "delivery_id" => %delivery.id,
                        "delivery_trigger" => %delivery.trigger,
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
                        "delivery_id" => %delivery.id,
                        "delivery_trigger" => %delivery.trigger,
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

        Ok(WebhookDeliveryAttempt {
            delivery_id: delivery.id,
            attempt: SqlU8::new(delivery.attempts.0 + 1),
            result: delivery_result,
            response_status: status.map(|s| s.as_u16() as i16),
            response_duration,
            time_created: chrono::Utc::now(),
        })
    }
}
