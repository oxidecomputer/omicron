// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Webhooks

use httpmock::prelude::*;
use nexus_db_queries::context::OpContext;
use nexus_test_utils::background::activate_background_task;
use nexus_test_utils::resource_helpers;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::{params, views};
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_uuid_kinds::WebhookEventUuid;

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
            events: vec!["test".to_string()],
            disable_probes: false,
        },
    )
    .await;
    dbg!(&webhook);

    let mock = server
        .mock_async(|when, then| {
            let body = serde_json::json!({
                "event_class": "test",
                "event_id": id,
                "data": {
                    "hello_world": true,
                }
            })
            .to_string();
            when.method(POST)
                .path("/webhooks")
                .json_body_includes(body)
                .header("x-oxide-event-class", "test")
                .header("x-oxide-event-id", id.to_string())
                .header("x-oxide-webhook-id", webhook.id.to_string())
                .header("content-type", "application/json")
                // This should be present, but we don't know what its' value is
                // going to be, so just assert that it's there.
                .header_exists("x-oxide-delivery-id");
            then.status(200);
        })
        .await;

    // Publish an event
    let event = nexus
        .webhook_event_publish(
            &opctx,
            id,
            "test".to_string(),
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
