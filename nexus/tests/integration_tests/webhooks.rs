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

#[nexus_test]
async fn demo(cptestctx: &ControlPlaneTestContext) {
    let nexus = cptestctx.server.server_context().nexus.clone();
    let internal_client = &cptestctx.internal_client;

    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    use dropshot::endpoint;
    use dropshot::ApiDescription;
    use dropshot::HttpError;
    use dropshot::HttpResponseUpdatedNoContent;
    use dropshot::RequestContext;
    use dropshot::TypedBody;
    use schemars::JsonSchema;
    use serde::Deserialize;
    use serde::Serialize;
    use uuid::Uuid;

    #[derive(Debug, Deserialize, Serialize, JsonSchema)]
    struct WebhookEvent {
        event_id: Uuid,
        event_class: String,
        data: serde_json::Value,
        delivery: DeliveryMetadata,
    }

    #[derive(Debug, Deserialize, Serialize, JsonSchema)]
    struct DeliveryMetadata {
        id: Uuid,
        webhook_id: Uuid,
        sent_at: String,
    }

    #[endpoint {
        method = POST,
        path = "/webhook",
    }]
    async fn webhook_post(
        rqctx: RequestContext<()>,
        body: TypedBody<WebhookEvent>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let body = body.into_inner();
        slog::info!(&rqctx.log, "received webhook POST: {body:#?}");
        Ok(HttpResponseUpdatedNoContent())
    }

    let log = {
        use omicron_common::FileKv;
        use slog::Drain;
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        let drain = slog::LevelFilter::new(drain, slog::Level::Info).fuse();
        slog::Logger::root(drain.fuse(), slog::o!(FileKv))
    };

    let mut api = ApiDescription::new();
    api.register(webhook_post).unwrap();

    let server = dropshot::ServerBuilder::new(
        api,
        (),
        log.new(slog::o!("component" => "webhook_rx")),
    )
    .config(dropshot::ConfigDropshot {
        bind_address: "127.0.0.1:8042".parse().unwrap(),
        request_body_max_bytes: 4096 * 8,
        default_handler_task_mode: dropshot::HandlerTaskMode::Detached,
        log_headers: vec![
            "x-oxide-event-class".to_string(),
            "x-oxide-event-id".to_string(),
            "x-oxide-delivery-id".to_string(),
            "x-oxide-webhook-id".to_string(),
        ],
    })
    .start()
    .unwrap();
    let log = log.new(slog::o!("component" => "demo"));
    let srv = tokio::spawn(server);

    // Create a webhook receiver.
    let webhook = webhook_create(
        &cptestctx,
        &params::WebhookCreate {
            identity: IdentityMetadataCreateParams {
                name: "my-great-webhook".parse().unwrap(),
                description: String::from("my great webhook"),
            },
            endpoint: "http://127.0.0.1:8042/webhook".parse().unwrap(),
            secrets: vec!["my cool secret".to_string()],
            events: vec![
                "test".to_string(),
                "test.foo".to_string(),
                "test.**.baz".to_string(),
            ],
            disable_probes: false,
        },
    )
    .await;
    slog::info!(&log, "created webhook receiver: {webhook:#?}");

    // Publish an event
    let id = WebhookEventUuid::new_v4();
    let event = nexus
        .webhook_event_publish(
            &opctx,
            id,
            "test.foo".to_string(),
            serde_json::json!({ "answer": 42 }),
        )
        .await;
    slog::info!(&log, "published event: {event:#?}");
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    let id = WebhookEventUuid::new_v4();
    let event = nexus
        .webhook_event_publish(
            &opctx,
            id,
            "test.bar".to_string(),
            serde_json::json!({"will_this_be_received": false}),
        )
        .await;
    slog::info!(&log, "published event: {event:#?}");
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    let id = WebhookEventUuid::new_v4();
    let event = nexus
        .webhook_event_publish(
            &opctx,
            id,
            "test.foo.baz".to_string(),
            serde_json::json!({"question": "life, the universe, and everything", "is_elizas_demo_cool": true }),
        )
        .await;
    slog::info!(&log, "published event: {event:#?}");

    activate_background_task(internal_client, "webhook_dispatcher").await;
    activate_background_task(internal_client, "webhook_deliverator").await;

    srv.await.unwrap().unwrap();
}
