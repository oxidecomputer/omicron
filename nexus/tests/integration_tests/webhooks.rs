// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Webhooks

use hmac::{Hmac, Mac};
use httpmock::prelude::*;
use nexus_db_model::WebhookEventClass;
use nexus_db_queries::context::OpContext;
use nexus_test_utils::background::activate_background_task;
use nexus_test_utils::resource_helpers;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::{params, views};
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_uuid_kinds::WebhookEventUuid;
use sha2::Sha256;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

async fn webhook_create(
    ctx: &ControlPlaneTestContext,
    params: &params::WebhookCreate,
) -> views::Webhook {
    resource_helpers::object_create::<params::WebhookCreate, views::Webhook>(
        &ctx.external_client,
        "/experimental/v1/webhooks",
        params,
    )
    .await
}

fn is_valid_for_webhook(
    webhook: &views::Webhook,
) -> impl FnOnce(httpmock::When) -> httpmock::When {
    let path = webhook.endpoint.path().to_string();
    let id = webhook.id.to_string();
    move |when| {
        when.path(path)
            .header("x-oxide-webhook-id", id)
            .header_exists("x-oxide-delivery-id")
            .header_exists("x-oxide-signature")
            .header("content-type", "application/json")
    }
}

fn signature_verifies(
    secret_id: String,
    secret: Vec<u8>,
) -> impl Fn(&HttpMockRequest) -> bool {
    move |req| {
        let hdrs = req.headers();
        let Some(sig_hdr) = hdrs.get_all("x-oxide-signature").iter().filter_map(|hdr| {
                // Signature header format:
                // a={algorithm}&id={secret_id}&s={signature}
                let hdr = dbg!(hdr.to_str()).expect("all x-oxide-signature headers should be valid utf-8");
                // Strip the expected algorithm part. Note that we only support
                // SHA256 for now. Panic if this is invalid.
                let hdr = hdr.strip_prefix("a=sha256").expect("all x-oxide-signature headers should have the SHA256 algorithm");
                // Strip the leading `&id=` for the ID part, panicking if this
                // is not found.
                let hdr = hdr.strip_prefix("&id=").expect("all x-oxide-signature headers should have a secret ID ID part");
                // If the ID isn't the one we're looking for, we want to keep
                // going, so just return `None` here
                let hdr = hdr.strip_prefix(secret_id.as_str())?;
                // Finally, extract the signature part by stripping the &s=
                // prefix.
                hdr.strip_prefix("&s=")
            }).next() else {
                panic!("no x-oxide-signature header for secret with ID {secret_id} found");
            };
        let sig_bytes = hex::decode(sig_hdr)
            .expect("x-oxide-signature signature value should be a hex string");
        let mut mac = Hmac::<Sha256>::new_from_slice(&secret[..])
            .expect("HMAC secrets can be any length");
        mac.update(req.body().as_ref());
        mac.verify_slice(&sig_bytes).is_ok()
    }
}

#[nexus_test]
async fn test_event_delivery(cptestctx: &ControlPlaneTestContext) {
    let nexus = cptestctx.server.server_context().nexus.clone();
    let internal_client = &cptestctx.internal_client;

    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    let server = httpmock::MockServer::start_async().await;

    let id = WebhookEventUuid::new_v4();
    let endpoint =
        server.url("/webhooks").parse().expect("this should be a valid URL");

    // Create a webhook receiver.
    let webhook = webhook_create(
        &cptestctx,
        &params::WebhookCreate {
            identity: IdentityMetadataCreateParams {
                name: "my-great-webhook".parse().unwrap(),
                description: String::from("my great webhook"),
            },
            endpoint,
            secrets: vec!["my cool secret".to_string()],
            events: vec!["test.foo".to_string()],
            disable_probes: false,
        },
    )
    .await;
    dbg!(&webhook);

    let mock = {
        let webhook = webhook.clone();
        server
            .mock_async(move |when, then| {
                let body = serde_json::json!({
                    "event_class": "test.foo",
                    "event_id": id,
                    "data": {
                        "hello_world": true,
                    }
                })
                .to_string();
                when.method(POST)
                    .and(is_valid_for_webhook(&webhook))
                    .is_true(signature_verifies(
                        webhook.secrets[0].id.to_string(),
                        "my cool secret".as_bytes().to_vec(),
                    ))
                    .json_body_includes(body)
                    .header("x-oxide-event-class", "test.foo")
                    .header("x-oxide-event-id", id.to_string());
                then.status(200);
            })
            .await
    };

    // Publish an event
    let event = nexus
        .webhook_event_publish(
            &opctx,
            id,
            WebhookEventClass::TestFoo,
            serde_json::json!({"hello_world": true}),
        )
        .await
        .expect("event should be published successfully");
    dbg!(event);

    dbg!(activate_background_task(internal_client, "webhook_dispatcher").await);
    dbg!(
        activate_background_task(internal_client, "webhook_deliverator").await
    );

    mock.assert_async().await;
}
