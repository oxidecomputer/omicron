// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Webhooks

use dropshot::test_util::ClientTestContext;
use hmac::{Hmac, Mac};
use httpmock::prelude::*;
use nexus_db_model::WebhookEventClass;
use nexus_db_queries::context::OpContext;
use nexus_test_utils::background::activate_background_task;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::resource_helpers;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::{params, views};
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::NameOrId;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::WebhookEventUuid;
use omicron_uuid_kinds::WebhookReceiverUuid;
use sha2::Sha256;
use std::time::Duration;
use uuid::Uuid;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

const RECEIVERS_BASE_PATH: &str = "/v1/webhooks/receivers";
const SECRETS_BASE_PATH: &str = "/v1/webhooks/secrets";
const DELIVERIES_BASE_PATH: &str = "/v1/webhooks/deliveries";

async fn webhook_create(
    ctx: &ControlPlaneTestContext,
    params: &params::WebhookCreate,
) -> views::WebhookReceiver {
    resource_helpers::object_create::<
        params::WebhookCreate,
        views::WebhookReceiver,
    >(&ctx.external_client, RECEIVERS_BASE_PATH, params)
    .await
}

fn get_webhooks_url(name_or_id: impl Into<NameOrId>) -> String {
    let name_or_id = name_or_id.into();
    format!("{RECEIVERS_BASE_PATH}/{name_or_id}")
}

async fn webhook_get(
    client: &ClientTestContext,
    webhook_url: &str,
) -> views::WebhookReceiver {
    webhook_get_as(client, webhook_url, AuthnMode::PrivilegedUser).await
}

async fn webhook_get_as(
    client: &ClientTestContext,
    webhook_url: &str,
    authn_as: AuthnMode,
) -> views::WebhookReceiver {
    NexusRequest::object_get(client, &webhook_url)
        .authn_as(authn_as)
        .execute()
        .await
        .unwrap()
        .parsed_body()
        .unwrap()
}

async fn webhook_rx_list(
    client: &ClientTestContext,
) -> Vec<views::WebhookReceiver> {
    resource_helpers::objects_list_page_authz::<views::WebhookReceiver>(
        client,
        RECEIVERS_BASE_PATH,
    )
    .await
    .items
}

async fn webhook_secrets_get(
    client: &ClientTestContext,
    webhook_name_or_id: impl Into<NameOrId>,
) -> views::WebhookSecrets {
    let name_or_id = webhook_name_or_id.into();
    NexusRequest::object_get(
        client,
        &format!("{SECRETS_BASE_PATH}/?receiver={name_or_id}"),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap()
}

fn resend_url(
    webhook_name_or_id: impl Into<NameOrId>,
    event_id: WebhookEventUuid,
) -> String {
    let rx = webhook_name_or_id.into();
    format!("{DELIVERIES_BASE_PATH}/{event_id}/resend?receiver={rx}")
}
async fn webhook_delivery_resend(
    client: &ClientTestContext,
    webhook_name_or_id: impl Into<NameOrId>,
    event_id: WebhookEventUuid,
) -> views::WebhookDeliveryId {
    let req = RequestBuilder::new(
        client,
        http::Method::POST,
        &resend_url(webhook_name_or_id, event_id),
    )
    .body::<serde_json::Value>(None)
    .expect_status(Some(http::StatusCode::CREATED));
    NexusRequest::new(req)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap()
        .parsed_body()
        .unwrap()
}

async fn webhook_delivery_resend_error(
    client: &ClientTestContext,
    webhook_name_or_id: impl Into<NameOrId>,
    event_id: WebhookEventUuid,
    status: http::StatusCode,
) -> dropshot::HttpErrorResponseBody {
    let req = RequestBuilder::new(
        client,
        http::Method::POST,
        &resend_url(webhook_name_or_id, event_id),
    )
    .body::<serde_json::Value>(None)
    .expect_status(Some(status));
    NexusRequest::new(req)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap()
        .parsed_body()
        .unwrap()
}

fn my_great_webhook_params(
    mock: &httpmock::MockServer,
) -> params::WebhookCreate {
    params::WebhookCreate {
        identity: IdentityMetadataCreateParams {
            name: "my-great-webhook".parse().unwrap(),
            description: String::from("my great webhook"),
        },
        endpoint: mock
            .url("/webhooks")
            .parse()
            .expect("this should be a valid URL"),
        secrets: vec![MY_COOL_SECRET.to_string()],
        events: vec!["test.foo".to_string()],
    }
}

const MY_COOL_SECRET: &str = "my cool secret";

async fn secret_add(
    ctx: &ControlPlaneTestContext,
    webhook_id: WebhookReceiverUuid,
    params: &params::WebhookSecretCreate,
) -> views::WebhookSecretId {
    resource_helpers::object_create::<
        params::WebhookSecretCreate,
        views::WebhookSecretId,
    >(
        &ctx.external_client,
        &format!("{SECRETS_BASE_PATH}/?receiver={webhook_id}"),
        params,
    )
    .await
}

async fn webhook_send_probe(
    ctx: &ControlPlaneTestContext,
    webhook_id: &WebhookReceiverUuid,
    resend: bool,
    status: http::StatusCode,
) -> views::WebhookProbeResult {
    let pathparams = if resend { "?resend=true" } else { "" };
    let path = format!("{RECEIVERS_BASE_PATH}/{webhook_id}/probe{pathparams}");
    NexusRequest::new(
        RequestBuilder::new(&ctx.external_client, http::Method::POST, &path)
            .expect_status(Some(status)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap_or_else(|e| {
        panic!("failed to make \"POST\" request to {path}: {e}")
    })
    .parsed_body()
    .unwrap()
}

fn is_valid_for_webhook(
    webhook: &views::WebhookReceiver,
) -> impl FnOnce(httpmock::When) -> httpmock::When {
    let path = webhook.endpoint.path().to_string();
    let id = webhook.identity.id.to_string();
    move |when| {
        when.path(path)
            .header("x-oxide-webhook-id", id)
            .header_exists("x-oxide-delivery-id")
            .header_exists("x-oxide-signature")
            .header("content-type", "application/json")
    }
}

fn signature_verifies(
    secret_id: Uuid,
    secret: Vec<u8>,
) -> impl Fn(&HttpMockRequest) -> bool {
    let secret_id = secret_id.to_string();
    move |req| {
        // N.B. that `HttpMockRequest::headers_vec()`, which returns a
        // `Vec<(String, String)>` is used here, rather than
        // `HttpMockRequest::headers()`, which returns a `HeaderMap`. This is
        // currently necessary because of a `httpmock` bug where, when multiple
        // values for the same header are present in the request, the map
        // returned by `headers()` will only contain one of those values. See:
        // https://github.com/alexliesenfeld/httpmock/issues/119
        let hdrs = req.headers_vec();
        let Some(sig_hdr) = hdrs.iter().find_map(|(name, hdr)| {
            if name != "x-oxide-signature" {
                return None;
            }
            // Signature header format:
            // a={algorithm}&id={secret_id}&s={signature}

            // Strip the expected algorithm part. Note that we only support
            // SHA256 for now. Panic if this is invalid.
            let hdr = hdr
                .strip_prefix("a=sha256")
                .expect("all x-oxide-signature headers should be SHA256");
            // Strip the leading `&id=` for the ID part, panicking if this
            // is not found.
            let hdr = hdr.strip_prefix("&id=").expect(
                "all x-oxide-signature headers should have a secret ID part",
            );
            // If the ID isn't the one we're looking for, we want to keep
            // going, so just return `None` here
            let hdr = hdr.strip_prefix(secret_id.as_str())?;
            // Finally, extract the signature part by stripping the &s=
            // prefix.
            hdr.strip_prefix("&s=")
        }) else {
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
async fn test_webhook_receiver_get(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    let server = httpmock::MockServer::start_async().await;

    // Create a webhook receiver.
    let created_webhook =
        webhook_create(&cptestctx, &my_great_webhook_params(&server)).await;
    dbg!(&created_webhook);

    // Fetch the receiver by ID.
    let by_id_url = get_webhooks_url(created_webhook.identity.id);
    let webhook_view = webhook_get(client, &by_id_url).await;
    assert_eq!(created_webhook, webhook_view);

    // Fetch the receiver by name.
    let by_name_url = get_webhooks_url(created_webhook.identity.name.clone());
    let webhook_view = webhook_get(client, &by_name_url).await;
    assert_eq!(created_webhook, webhook_view);
}

#[nexus_test]
async fn test_webhook_receiver_create_delete(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let server = httpmock::MockServer::start_async().await;

    // Create a webhook receiver.
    let created_webhook =
        webhook_create(&cptestctx, &my_great_webhook_params(&server)).await;
    dbg!(&created_webhook);

    resource_helpers::object_delete(
        client,
        &format!("{RECEIVERS_BASE_PATH}/{}", created_webhook.identity.name),
    )
    .await;

    // It should be gone now.
    resource_helpers::object_delete_error(
        client,
        &format!("{RECEIVERS_BASE_PATH}/{}", created_webhook.identity.name),
        http::StatusCode::NOT_FOUND,
    )
    .await;
}
#[nexus_test]
async fn test_webhook_receiver_names_are_unique(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let server = httpmock::MockServer::start_async().await;

    // Create a webhook receiver.
    let created_webhook =
        webhook_create(&cptestctx, &my_great_webhook_params(&server)).await;
    dbg!(&created_webhook);

    let error = resource_helpers::object_create_error(
        &client,
        RECEIVERS_BASE_PATH,
        &my_great_webhook_params(&server),
        http::StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(
        dbg!(&error).message,
        "already exists: webhook-receiver \"my-great-webhook\""
    );
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

    // Create a webhook receiver.
    let webhook =
        webhook_create(&cptestctx, &my_great_webhook_params(&server)).await;
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
                    .header("x-oxide-event-class", "test.foo")
                    .header("x-oxide-event-id", id.to_string())
                    .and(is_valid_for_webhook(&webhook))
                    .is_true(signature_verifies(
                        webhook.secrets[0].id,
                        MY_COOL_SECRET.as_bytes().to_vec(),
                    ))
                    .json_body_includes(body);
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

#[nexus_test]
async fn test_multiple_secrets(cptestctx: &ControlPlaneTestContext) {
    let nexus = cptestctx.server.server_context().nexus.clone();
    let internal_client = &cptestctx.internal_client;

    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    let server = httpmock::MockServer::start_async().await;

    let id = WebhookEventUuid::new_v4();
    let endpoint =
        server.url("/webhooks").parse().expect("this should be a valid URL");

    const SECRET1: &str = "it's an older code, sir, but it checks out";
    const SECRET2: &str = "Joshua";
    const SECRET3: &str = "Setec Astronomy";

    // Create a webhook receiver.
    let webhook = webhook_create(
        &cptestctx,
        &params::WebhookCreate {
            identity: IdentityMetadataCreateParams {
                name: "my-great-webhook".parse().unwrap(),
                description: String::from("my great webhook"),
            },
            endpoint,
            secrets: vec![SECRET1.to_string()],
            events: vec!["test.foo".to_string()],
        },
    )
    .await;
    dbg!(&webhook);
    let rx_id = WebhookReceiverUuid::from_untyped_uuid(webhook.identity.id);

    let secret1_id = webhook.secrets[0].id;

    let client = &cptestctx.external_client;
    let assert_secrets_get = |mut expected: Vec<Uuid>| async move {
        let mut actual = webhook_secrets_get(client, rx_id.into_untyped_uuid())
            .await
            .secrets
            .into_iter()
            .map(|secret| secret.id)
            .collect::<Vec<_>>();
        actual.sort();
        expected.sort();
        assert_eq!(expected, actual);
    };

    assert_secrets_get(vec![secret1_id]).await;

    // Add a second secret to the webhook receiver.
    let secret2_id = dbg!(
        secret_add(
            &cptestctx,
            rx_id,
            &params::WebhookSecretCreate { secret: SECRET2.to_string() },
        )
        .await
    )
    .id;
    assert_secrets_get(vec![secret1_id, secret2_id]).await;

    // And a third one, just for kicks.
    let secret3_id = dbg!(
        secret_add(
            &cptestctx,
            rx_id,
            &params::WebhookSecretCreate { secret: SECRET3.to_string() },
        )
        .await
    )
    .id;
    assert_secrets_get(vec![secret1_id, secret2_id, secret3_id]).await;

    let mock = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-oxide-event-class", "test.foo")
                .header("x-oxide-event-id", id.to_string())
                .and(is_valid_for_webhook(&webhook))
                // There should be a signature header present for all three
                // secrets, and they should all verify the contents of the
                // webhook request.
                .is_true(signature_verifies(
                    secret1_id,
                    SECRET1.as_bytes().to_vec(),
                ))
                .is_true(signature_verifies(
                    secret2_id,
                    SECRET2.as_bytes().to_vec(),
                ))
                .is_true(signature_verifies(
                    secret3_id,
                    SECRET3.as_bytes().to_vec(),
                ));
            then.status(200);
        })
        .await;

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

#[nexus_test]
async fn test_multiple_receivers(cptestctx: &ControlPlaneTestContext) {
    let nexus = cptestctx.server.server_context().nexus.clone();
    let internal_client = &cptestctx.internal_client;
    let client = &cptestctx.external_client;

    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    let bar_event_id = WebhookEventUuid::new_v4();
    let baz_event_id = WebhookEventUuid::new_v4();

    let assert_webhook_rx_list_matches =
        |mut expected: Vec<views::WebhookReceiver>| async move {
            let mut actual = webhook_rx_list(client).await;
            actual.sort_by_key(|rx| rx.identity.id);
            expected.sort_by_key(|rx| rx.identity.id);
            assert_eq!(expected, actual);
        };

    // Create three webhook receivers
    let srv_bar = httpmock::MockServer::start_async().await;
    const BAR_SECRET: &str = "this is bar's secret";
    let rx_bar = webhook_create(
        &cptestctx,
        &params::WebhookCreate {
            identity: IdentityMetadataCreateParams {
                name: "webhooked-on-phonics".parse().unwrap(),
                description: String::from("webhooked on phonics"),
            },
            endpoint: srv_bar
                .url("/webhooks")
                .parse()
                .expect("this should be a valid URL"),
            secrets: vec![BAR_SECRET.to_string()],
            events: vec!["test.foo.bar".to_string()],
        },
    )
    .await;
    dbg!(&rx_bar);
    assert_webhook_rx_list_matches(vec![rx_bar.clone()]).await;
    let mock_bar = {
        let webhook = rx_bar.clone();
        srv_bar
            .mock_async(move |when, then| {
                when.method(POST)
                    .header("x-oxide-event-class", "test.foo.bar")
                    .header("x-oxide-event-id", bar_event_id.to_string())
                    .and(is_valid_for_webhook(&webhook))
                    .is_true(signature_verifies(
                        webhook.secrets[0].id,
                        BAR_SECRET.as_bytes().to_vec(),
                    ));
                then.status(200);
            })
            .await
    };

    let srv_baz = httpmock::MockServer::start_async().await;
    const BAZ_SECRET: &str = "this is baz's secret";
    let rx_baz = webhook_create(
        &cptestctx,
        &params::WebhookCreate {
            identity: IdentityMetadataCreateParams {
                name: "webhook-line-and-sinker".parse().unwrap(),
                description: String::from("webhook, line, and sinker"),
            },
            endpoint: srv_baz
                .url("/webhooks")
                .parse()
                .expect("this should be a valid URL"),
            secrets: vec![BAZ_SECRET.to_string()],
            events: vec!["test.foo.baz".to_string()],
        },
    )
    .await;
    dbg!(&rx_baz);
    assert_webhook_rx_list_matches(vec![rx_bar.clone(), rx_baz.clone()]).await;
    let mock_baz = {
        let webhook = rx_baz.clone();
        srv_baz
            .mock_async(move |when, then| {
                when.method(POST)
                    .header("x-oxide-event-class", "test.foo.baz")
                    .header("x-oxide-event-id", baz_event_id.to_string())
                    .and(is_valid_for_webhook(&webhook))
                    .is_true(signature_verifies(
                        webhook.secrets[0].id,
                        BAZ_SECRET.as_bytes().to_vec(),
                    ));
                then.status(200);
            })
            .await
    };

    let srv_star = httpmock::MockServer::start_async().await;
    const STAR_SECRET: &str = "this is star's secret";
    let rx_star = webhook_create(
        &cptestctx,
        &params::WebhookCreate {
            identity: IdentityMetadataCreateParams {
                name: "globulated".parse().unwrap(),
                description: String::from("this one has globs"),
            },
            endpoint: srv_star
                .url("/webhooks")
                .parse()
                .expect("this should be a valid URL"),
            secrets: vec![STAR_SECRET.to_string()],
            events: vec!["test.foo.*".to_string()],
        },
    )
    .await;
    dbg!(&rx_star);
    assert_webhook_rx_list_matches(vec![
        rx_bar.clone(),
        rx_baz.clone(),
        rx_star.clone(),
    ])
    .await;
    let mock_star = {
        let webhook = rx_star.clone();
        srv_star
            .mock_async(move |when, then| {
                when.method(POST)
                    .header_matches(
                        "x-oxide-event-class",
                        "test\\.foo\\.ba[rz]",
                    )
                    .header_exists("x-oxide-event-id")
                    .and(is_valid_for_webhook(&webhook))
                    .is_true(signature_verifies(
                        webhook.secrets[0].id,
                        STAR_SECRET.as_bytes().to_vec(),
                    ));
                then.status(200);
            })
            .await
    };

    // Publish a test.foo.bar event
    let event = nexus
        .webhook_event_publish(
            &opctx,
            bar_event_id,
            WebhookEventClass::TestFooBar,
            serde_json::json!({"lol": "webhooked on phonics"}),
        )
        .await
        .expect("event should be published successfully");
    dbg!(event);
    // Publish a test.foo.baz event
    let event = nexus
        .webhook_event_publish(
            &opctx,
            baz_event_id,
            WebhookEventClass::TestFooBaz,
            serde_json::json!({"lol": "webhook, line, and sinker"}),
        )
        .await
        .expect("event should be published successfully");
    dbg!(event);

    dbg!(activate_background_task(internal_client, "webhook_dispatcher").await);
    dbg!(
        activate_background_task(internal_client, "webhook_deliverator").await
    );

    // The `test.foo.bar` receiver should have received 1 event.
    mock_bar.assert_calls_async(1).await;

    // The `test.foo.baz` receiver should have received 1 event.
    mock_baz.assert_calls_async(1).await;

    // The `test.foo.*` receiver should have received both events.
    mock_star.assert_calls_async(2).await;
}

#[nexus_test]
async fn test_retry_backoff(cptestctx: &ControlPlaneTestContext) {
    let nexus = cptestctx.server.server_context().nexus.clone();
    let internal_client = &cptestctx.internal_client;

    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    let server = httpmock::MockServer::start_async().await;

    let id = WebhookEventUuid::new_v4();

    // Create a webhook receiver.
    let webhook =
        webhook_create(&cptestctx, &my_great_webhook_params(&server)).await;
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
                    .header("x-oxide-event-class", "test.foo")
                    .header("x-oxide-event-id", id.to_string())
                    .and(is_valid_for_webhook(&webhook))
                    .is_true(signature_verifies(
                        webhook.secrets[0].id,
                        MY_COOL_SECRET.as_bytes().to_vec(),
                    ))
                    .json_body_includes(body);
                then.status(500);
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

    mock.assert_calls_async(1).await;

    // Okay, we are now in backoff. Activate the deliverator again --- no new
    // event should be delivered.
    dbg!(
        activate_background_task(internal_client, "webhook_deliverator").await
    );
    // Activating the deliverator whilst in backoff should not send another
    // request.
    mock.assert_calls_async(1).await;
    mock.delete_async().await;

    // Okay, now let's return a different 5xx status.
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
                    .header("x-oxide-event-class", "test.foo")
                    .header("x-oxide-event-id", id.to_string())
                    .and(is_valid_for_webhook(&webhook))
                    .is_true(signature_verifies(
                        webhook.secrets[0].id,
                        MY_COOL_SECRET.as_bytes().to_vec(),
                    ))
                    .json_body_includes(body);
                then.status(503);
            })
            .await
    };

    // Wait out the backoff period for the first request.
    tokio::time::sleep(std::time::Duration::from_secs(15)).await;
    dbg!(
        activate_background_task(internal_client, "webhook_deliverator").await
    );
    mock.assert_calls_async(1).await;

    // Again, we should be in backoff, so no request will be sent.
    dbg!(
        activate_background_task(internal_client, "webhook_deliverator").await
    );
    mock.assert_calls_async(1).await;
    mock.delete_async().await;

    // Finally, allow the request to succeed.
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
                    .header("x-oxide-event-class", "test.foo")
                    .header("x-oxide-event-id", id.to_string())
                    .and(is_valid_for_webhook(&webhook))
                    .is_true(signature_verifies(
                        webhook.secrets[0].id,
                        MY_COOL_SECRET.as_bytes().to_vec(),
                    ))
                    .json_body_includes(body);
                then.status(200);
            })
            .await
    };

    //
    tokio::time::sleep(std::time::Duration::from_secs(15)).await;
    dbg!(
        activate_background_task(internal_client, "webhook_deliverator").await
    );
    mock.assert_calls_async(0).await;

    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    dbg!(
        activate_background_task(internal_client, "webhook_deliverator").await
    );
    mock.assert_async().await;
}

#[nexus_test]
async fn test_probe(cptestctx: &ControlPlaneTestContext) {
    let server = httpmock::MockServer::start_async().await;

    // Create a webhook receiver.
    let webhook =
        webhook_create(&cptestctx, &my_great_webhook_params(&server)).await;
    dbg!(&webhook);
    let rx_id = WebhookReceiverUuid::from_untyped_uuid(webhook.identity.id);

    let body = serde_json::json!({
        "event_class": "probe",
        "data": {}
    })
    .to_string();

    // First, configure the receiver server to return a successful response but
    // only after the delivery timeout has elapsed.
    let mock = {
        let webhook = webhook.clone();
        let body = body.clone();
        server
            .mock_async(move |when, then| {
                when.method(POST)
                    .header("x-oxide-event-class", "probe")
                    .and(is_valid_for_webhook(&webhook))
                    .is_true(signature_verifies(
                        webhook.secrets[0].id,
                        MY_COOL_SECRET.as_bytes().to_vec(),
                    ))
                    .json_body_includes(body);
                then
                    // Delivery timeout is 30 seconds.
                    // TODO(eliza): it would be really nice if this test didn't
                    // have to wait 30 seconds...
                    .delay(Duration::from_secs(35))
                    // After the timeout, return something that would be considered
                    // a success.
                    .status(200);
            })
            .await
    };

    // Send a probe. The probe should fail due to a timeout.
    let probe1 =
        webhook_send_probe(&cptestctx, &rx_id, false, http::StatusCode::OK)
            .await;
    dbg!(&probe1);

    mock.assert_async().await;

    assert_eq!(
        probe1.probe.attempts[0].result,
        views::WebhookDeliveryAttemptResult::FailedTimeout
    );
    assert_eq!(probe1.probe.event_class, "probe");
    assert_eq!(probe1.probe.trigger, views::WebhookDeliveryTrigger::Probe);
    assert_eq!(probe1.probe.state, views::WebhookDeliveryState::Failed);
    assert_eq!(
        probe1.resends_started, None,
        "we did not request events be resent"
    );

    // Next, configure the receiver server to return a 5xx error
    mock.delete_async().await;
    let mock = {
        let webhook = webhook.clone();
        let body = body.clone();
        server
            .mock_async(move |when, then| {
                when.method(POST)
                    .header("x-oxide-event-class", "probe")
                    .and(is_valid_for_webhook(&webhook))
                    .is_true(signature_verifies(
                        webhook.secrets[0].id,
                        MY_COOL_SECRET.as_bytes().to_vec(),
                    ))
                    .json_body_includes(body);
                then.status(503);
            })
            .await
    };

    let probe2 =
        webhook_send_probe(&cptestctx, &rx_id, false, http::StatusCode::OK)
            .await;
    dbg!(&probe2);

    mock.assert_async().await;
    assert_eq!(
        probe2.probe.attempts[0].result,
        views::WebhookDeliveryAttemptResult::FailedHttpError
    );
    assert_eq!(probe2.probe.event_class, "probe");
    assert_eq!(probe2.probe.trigger, views::WebhookDeliveryTrigger::Probe);
    assert_eq!(probe2.probe.state, views::WebhookDeliveryState::Failed);
    assert_ne!(
        probe2.probe.id, probe1.probe.id,
        "a new delivery ID should be assigned to each probe"
    );
    assert_eq!(
        probe2.resends_started, None,
        "we did not request events be resent"
    );

    mock.delete_async().await;
    // Finally, configure the receiver server to return a success.
    let mock = {
        let webhook = webhook.clone();
        let body = body.clone();
        server
            .mock_async(move |when, then| {
                when.method(POST)
                    .header("x-oxide-event-class", "probe")
                    .and(is_valid_for_webhook(&webhook))
                    .is_true(signature_verifies(
                        webhook.secrets[0].id,
                        MY_COOL_SECRET.as_bytes().to_vec(),
                    ))
                    .json_body_includes(body);
                then.status(200);
            })
            .await
    };

    let probe3 =
        webhook_send_probe(&cptestctx, &rx_id, false, http::StatusCode::OK)
            .await;
    dbg!(&probe3);
    mock.assert_async().await;
    assert_eq!(
        probe3.probe.attempts[0].result,
        views::WebhookDeliveryAttemptResult::Succeeded
    );
    assert_eq!(probe3.probe.event_class, "probe");
    assert_eq!(probe3.probe.trigger, views::WebhookDeliveryTrigger::Probe);
    assert_eq!(probe3.probe.state, views::WebhookDeliveryState::Delivered);
    assert_ne!(
        probe3.probe.id, probe1.probe.id,
        "a new delivery ID should be assigned to each probe"
    );
    assert_ne!(
        probe3.probe.id, probe2.probe.id,
        "a new delivery ID should be assigned to each probe"
    );
    assert_eq!(
        probe3.resends_started, None,
        "we did not request events be resent"
    );
}

#[nexus_test]
async fn test_probe_resends_failed_deliveries(
    cptestctx: &ControlPlaneTestContext,
) {
    let nexus = cptestctx.server.server_context().nexus.clone();
    let internal_client = &cptestctx.internal_client;
    let server = httpmock::MockServer::start_async().await;

    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    // Create a webhook receiver.
    let webhook =
        webhook_create(&cptestctx, &my_great_webhook_params(&server)).await;
    dbg!(&webhook);

    let event1_id = WebhookEventUuid::new_v4();
    let event2_id = WebhookEventUuid::new_v4();
    let mock = {
        let webhook = webhook.clone();
        server
            .mock_async(move |when, then| {
                when.method(POST)
                    .header("x-oxide-event-class", "test.foo")
                    // either event
                    .header_matches(
                        "x-oxide-event-id",
                        format!("({event1_id})|({event2_id})"),
                    )
                    .and(is_valid_for_webhook(&webhook))
                    .is_true(signature_verifies(
                        webhook.secrets[0].id,
                        MY_COOL_SECRET.as_bytes().to_vec(),
                    ));
                then.status(500);
            })
            .await
    };

    // Publish both events
    dbg!(nexus
        .webhook_event_publish(
            &opctx,
            event1_id,
            WebhookEventClass::TestFoo,
            serde_json::json!({"hello": "world"}),
        )
        .await
        .expect("event1 should be published successfully"));
    dbg!(nexus
        .webhook_event_publish(
            &opctx,
            event2_id,
            WebhookEventClass::TestFoo,
            serde_json::json!({"hello": "emeryville"}),
        )
        .await
        .expect("event2 should be published successfully"));

    dbg!(activate_background_task(internal_client, "webhook_dispatcher").await);
    dbg!(
        activate_background_task(internal_client, "webhook_deliverator").await
    );
    mock.assert_calls_async(2).await;

    // Backoff 1
    tokio::time::sleep(std::time::Duration::from_secs(11)).await;
    dbg!(
        activate_background_task(internal_client, "webhook_deliverator").await
    );
    mock.assert_calls_async(4).await;

    // Backoff 2
    tokio::time::sleep(std::time::Duration::from_secs(22)).await;
    dbg!(
        activate_background_task(internal_client, "webhook_deliverator").await
    );
    mock.assert_calls_async(6).await;

    mock.delete_async().await;

    // Allow a probe to succeed
    let probe_mock = {
        let webhook = webhook.clone();
        server
            .mock_async(move |when, then| {
                let body = serde_json::json!({
                    "event_class": "probe",
                    "data": {
                    }
                })
                .to_string();
                when.method(POST)
                    .header("x-oxide-event-class", "probe")
                    .and(is_valid_for_webhook(&webhook))
                    .is_true(signature_verifies(
                        webhook.secrets[0].id,
                        MY_COOL_SECRET.as_bytes().to_vec(),
                    ))
                    .json_body_includes(body);
                then.status(200);
            })
            .await
    };

    // Allow events to succeed.
    let mock = {
        let webhook = webhook.clone();
        server
            .mock_async(move |when, then| {
                when.method(POST)
                    .header("x-oxide-event-class", "test.foo")
                    // either event
                    .header_matches(
                        "x-oxide-event-id",
                        format!("({event1_id})|({event2_id})"),
                    )
                    .and(is_valid_for_webhook(&webhook))
                    .is_true(signature_verifies(
                        webhook.secrets[0].id,
                        MY_COOL_SECRET.as_bytes().to_vec(),
                    ));
                then.status(200);
            })
            .await
    };

    // Send a probe with ?resend=true
    let rx_id = WebhookReceiverUuid::from_untyped_uuid(webhook.identity.id);
    let probe =
        webhook_send_probe(&cptestctx, &rx_id, true, http::StatusCode::OK)
            .await;
    dbg!(&probe);
    probe_mock.assert_async().await;
    probe_mock.delete_async().await;
    assert_eq!(probe.probe.state, views::WebhookDeliveryState::Delivered);
    assert_eq!(probe.resends_started, Some(2));

    // Both events should be resent.
    dbg!(
        activate_background_task(internal_client, "webhook_deliverator").await
    );
    mock.assert_calls_async(2).await;
}

#[nexus_test]
async fn test_api_resends_failed_deliveries(
    cptestctx: &ControlPlaneTestContext,
) {
    let nexus = cptestctx.server.server_context().nexus.clone();
    let internal_client = &cptestctx.internal_client;
    let client = &cptestctx.external_client;
    let server = httpmock::MockServer::start_async().await;

    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    // Create a webhook receiver.
    let webhook =
        webhook_create(&cptestctx, &my_great_webhook_params(&server)).await;
    dbg!(&webhook);

    let event1_id = WebhookEventUuid::new_v4();
    let event2_id = WebhookEventUuid::new_v4();
    let body = serde_json::json!({
        "event_class": "test.foo",
        "event_id": event1_id,
        "data": {
            "hello_world": true,
        }
    })
    .to_string();
    let mock = {
        let webhook = webhook.clone();
        let body = body.clone();
        server
            .mock_async(move |when, then| {
                when.method(POST)
                    .header("x-oxide-event-class", "test.foo")
                    .header("x-oxide-event-id", event1_id.to_string())
                    .and(is_valid_for_webhook(&webhook))
                    .is_true(signature_verifies(
                        webhook.secrets[0].id,
                        MY_COOL_SECRET.as_bytes().to_vec(),
                    ))
                    .json_body_includes(body);
                then.status(500);
            })
            .await
    };

    // Publish an event
    let event1 = nexus
        .webhook_event_publish(
            &opctx,
            event1_id,
            WebhookEventClass::TestFoo,
            serde_json::json!({"hello_world": true}),
        )
        .await
        .expect("event should be published successfully");
    dbg!(event1);

    // Publish another event that our receiver is not subscribed to.
    let event2 = nexus
        .webhook_event_publish(
            &opctx,
            event2_id,
            WebhookEventClass::TestQuuxBar,
            serde_json::json!({"hello_world": true}),
        )
        .await
        .expect("event should be published successfully");
    dbg!(event2);

    dbg!(activate_background_task(internal_client, "webhook_dispatcher").await);
    dbg!(
        activate_background_task(internal_client, "webhook_deliverator").await
    );

    tokio::time::sleep(std::time::Duration::from_secs(11)).await;
    dbg!(
        activate_background_task(internal_client, "webhook_deliverator").await
    );
    tokio::time::sleep(std::time::Duration::from_secs(22)).await;
    dbg!(
        activate_background_task(internal_client, "webhook_deliverator").await
    );

    mock.assert_calls_async(3).await;
    mock.delete_async().await;

    let mock = {
        let webhook = webhook.clone();
        let body = body.clone();
        server
            .mock_async(move |when, then| {
                when.method(POST)
                    .header("x-oxide-event-class", "test.foo")
                    .header("x-oxide-event-id", event1_id.to_string())
                    .and(is_valid_for_webhook(&webhook))
                    .is_true(signature_verifies(
                        webhook.secrets[0].id,
                        MY_COOL_SECRET.as_bytes().to_vec(),
                    ))
                    .json_body_includes(body);
                then.status(200);
            })
            .await
    };

    // Try to resend event 1.
    let delivery =
        webhook_delivery_resend(client, webhook.identity.id, event1_id).await;
    dbg!(delivery);

    // Try to resend event 2. This should fail, as the receiver is not
    // subscribed to this event class.
    let error = webhook_delivery_resend_error(
        client,
        webhook.identity.id,
        event2_id,
        http::StatusCode::BAD_REQUEST,
    )
    .await;
    dbg!(error);

    dbg!(
        activate_background_task(internal_client, "webhook_deliverator").await
    );
    mock.assert_calls_async(1).await;
}
