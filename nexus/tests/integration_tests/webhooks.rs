// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Webhooks

use dropshot::test_util::ClientTestContext;
use hmac::{Hmac, Mac};
use httpmock::prelude::*;
use nexus_db_model::AlertClass;
use nexus_db_queries::context::OpContext;
use nexus_test_utils::background::activate_background_task;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::Collection;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::resource_helpers;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::{params, shared, views};
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::IdentityMetadataUpdateParams;
use omicron_common::api::external::NameOrId;
use omicron_uuid_kinds::AlertReceiverUuid;
use omicron_uuid_kinds::AlertUuid;
use omicron_uuid_kinds::GenericUuid;
use sha2::Sha256;
use std::time::Duration;
use uuid::Uuid;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

const ALERTS_BASE_PATH: &str = "/v1/alerts";
const ALERT_RECEIVERS_BASE_PATH: &str = "/v1/alert-receivers";
const WEBHOOK_RECEIVERS_BASE_PATH: &str = "/v1/webhook-receivers";
const SECRETS_BASE_PATH: &str = "/v1/webhook-secrets";

async fn webhook_create(
    ctx: &ControlPlaneTestContext,
    params: &params::WebhookCreate,
) -> views::WebhookReceiver {
    resource_helpers::object_create::<
        params::WebhookCreate,
        views::WebhookReceiver,
    >(&ctx.external_client, WEBHOOK_RECEIVERS_BASE_PATH, params)
    .await
}

fn alert_rx_url(name_or_id: impl Into<NameOrId>) -> String {
    let name_or_id = name_or_id.into();
    format!("{ALERT_RECEIVERS_BASE_PATH}/{name_or_id}")
}

async fn alert_rx_get(
    client: &ClientTestContext,
    webhook_url: &str,
) -> views::AlertReceiver {
    alert_rx_get_as(client, webhook_url, AuthnMode::PrivilegedUser).await
}

async fn alert_rx_get_as(
    client: &ClientTestContext,
    webhook_url: &str,
    authn_as: AuthnMode,
) -> views::AlertReceiver {
    NexusRequest::object_get(client, &webhook_url)
        .authn_as(authn_as)
        .execute()
        .await
        .unwrap()
        .parsed_body()
        .unwrap()
}

async fn alert_rx_list(
    client: &ClientTestContext,
) -> Vec<views::AlertReceiver> {
    resource_helpers::objects_list_page_authz::<views::AlertReceiver>(
        client,
        ALERT_RECEIVERS_BASE_PATH,
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
    alert_id: AlertUuid,
) -> String {
    let rx = webhook_name_or_id.into();
    format!("{ALERTS_BASE_PATH}/{alert_id}/resend?receiver={rx}")
}

async fn alert_deliveries_list(
    client: &ClientTestContext,
    webhook_name_or_id: impl Into<NameOrId>,
) -> Collection<views::AlertDelivery> {
    let mut rx_url = alert_rx_url(webhook_name_or_id);
    rx_url.push_str("/deliveries");
    NexusRequest::iter_collection_authn(client, &rx_url, "", None)
        .await
        .unwrap()
}

async fn alert_delivery_resend(
    client: &ClientTestContext,
    webhook_name_or_id: impl Into<NameOrId>,
    alert_id: AlertUuid,
) -> views::AlertDeliveryId {
    let req = RequestBuilder::new(
        client,
        http::Method::POST,
        &resend_url(webhook_name_or_id, alert_id),
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
    alert_id: AlertUuid,
    status: http::StatusCode,
) -> dropshot::HttpErrorResponseBody {
    let req = RequestBuilder::new(
        client,
        http::Method::POST,
        &resend_url(webhook_name_or_id, alert_id),
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
        identity: my_great_webhook_identity(),
        endpoint: mock
            .url("/webhooks")
            .parse()
            .expect("this should be a valid URL"),
        secrets: vec![MY_COOL_SECRET.to_string()],
        subscriptions: vec!["test.foo".parse().unwrap()],
    }
}

fn my_great_webhook_identity() -> IdentityMetadataCreateParams {
    IdentityMetadataCreateParams {
        name: "my-great-webhook".parse().unwrap(),
        description: String::from("my great webhook"),
    }
}

const MY_COOL_SECRET: &str = "my cool secret";

async fn secret_add(
    ctx: &ControlPlaneTestContext,
    webhook_id: AlertReceiverUuid,
    params: &params::WebhookSecretCreate,
) -> views::WebhookSecret {
    resource_helpers::object_create::<
        params::WebhookSecretCreate,
        views::WebhookSecret,
    >(
        &ctx.external_client,
        &format!("{SECRETS_BASE_PATH}/?receiver={webhook_id}"),
        params,
    )
    .await
}

async fn subscription_add(
    ctx: &ControlPlaneTestContext,
    webhook_id: AlertReceiverUuid,
    subscription: &shared::AlertSubscription,
) -> views::AlertSubscriptionCreated {
    resource_helpers::object_create(
        &ctx.external_client,
        &format!("{ALERT_RECEIVERS_BASE_PATH}/{webhook_id}/subscriptions"),
        &params::AlertSubscriptionCreate { subscription: subscription.clone() },
    )
    .await
}

async fn subscription_remove(
    ctx: &ControlPlaneTestContext,
    webhook_id: AlertReceiverUuid,
    subscription: &shared::AlertSubscription,
) {
    resource_helpers::object_delete(
        &ctx.external_client,
        &subscription_remove_url(webhook_id, subscription),
    )
    .await
}

fn subscription_remove_url(
    webhook_id: AlertReceiverUuid,
    subscription: &shared::AlertSubscription,
) -> String {
    format!(
        "{ALERT_RECEIVERS_BASE_PATH}/{webhook_id}/subscriptions/{subscription}"
    )
}

async fn alert_receiver_send_probe(
    ctx: &ControlPlaneTestContext,
    webhook_id: &AlertReceiverUuid,
    resend: bool,
    status: http::StatusCode,
) -> views::AlertProbeResult {
    let pathparams = if resend { "?resend=true" } else { "" };
    let path =
        format!("{ALERT_RECEIVERS_BASE_PATH}/{webhook_id}/probe{pathparams}");
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
    let path = webhook.config.endpoint.path().to_string();
    let id = webhook.identity.id.to_string();
    move |when| {
        when.path(path)
            .header("x-oxide-receiver-id", id)
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
            panic!(
                "no x-oxide-signature header for secret with ID {secret_id} found"
            );
        };
        let sig_bytes = hex::decode(sig_hdr)
            .expect("x-oxide-signature signature value should be a hex string");
        let mut mac = Hmac::<Sha256>::new_from_slice(&secret[..])
            .expect("HMAC secrets can be any length");
        mac.update(req.body().as_ref());
        mac.verify_slice(&sig_bytes).is_ok()
    }
}

struct ExpectAttempt {
    result: views::WebhookDeliveryAttemptResult,
    status: Option<u16>,
}

/// Helper for making assertions about webhook deliveries, while ignoring things
/// such as timestamps, which are variable.
#[track_caller]
fn expect_delivery_attempts(
    actual: &views::AlertDeliveryAttempts,
    expected: &[ExpectAttempt],
) {
    let views::AlertDeliveryAttempts::Webhook(actual) = actual;
    assert_eq!(
        actual.len(),
        expected.len(),
        "expected {} delivery attempts, found: {actual:?}",
        expected.len()
    );
    for (n, (actual, ExpectAttempt { result, status })) in
        actual.iter().zip(expected.iter()).enumerate()
    {
        dbg!(&actual);
        assert_eq!(actual.attempt, n + 1);
        assert_eq!(&actual.result, result);
        assert_eq!(actual.response.as_ref().map(|rsp| rsp.status), *status);
    }
}

#[nexus_test]
async fn test_webhook_receiver_get(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    // Create a webhook receiver.
    let created_webhook = webhook_create(
        &cptestctx,
        &params::WebhookCreate {
            identity: my_great_webhook_identity(),
            endpoint: "https://example.com/webhooks"
                .parse()
                .expect("this should be a valid URL"),
            secrets: vec![MY_COOL_SECRET.to_string()],
            subscriptions: vec!["test.foo".parse().unwrap()],
        },
    )
    .await;
    dbg!(&created_webhook);

    // Fetch the receiver by ID.
    let by_id_url = alert_rx_url(created_webhook.identity.id);
    let webhook_view = alert_rx_get(client, &by_id_url).await;
    assert_eq!(created_webhook, webhook_view);

    // Fetch the receiver by name.
    let by_name_url = alert_rx_url(created_webhook.identity.name.clone());
    let webhook_view = alert_rx_get(client, &by_name_url).await;
    assert_eq!(created_webhook, webhook_view);
}

#[nexus_test]
async fn test_webhook_receiver_create_update(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Create a webhook receiver.
    let created_webhook = webhook_create(
        &cptestctx,
        &params::WebhookCreate {
            identity: my_great_webhook_identity(),
            endpoint: "https://example.com/webhooks"
                .parse()
                .expect("this should be a valid URL"),
            secrets: vec![MY_COOL_SECRET.to_string()],
            subscriptions: vec!["test.foo".parse().unwrap()],
        },
    )
    .await;
    dbg!(&created_webhook);

    let hook_name = created_webhook.identity.name.clone();
    let hook_url = format!("{WEBHOOK_RECEIVERS_BASE_PATH}/{hook_name}");

    NexusRequest::new(
        RequestBuilder::new(&client, http::Method::PUT, &hook_url)
            .body(Some(&params::WebhookReceiverUpdate {
                identity: IdentityMetadataUpdateParams {
                    name: None,
                    description: Some(String::from("an updated description")),
                },
                endpoint: Some(
                    "https://example.com/webhooks/updated".parse().unwrap(),
                ),
            }))
            .expect_status(Some(http::StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    let receiver_url = alert_rx_url(created_webhook.identity.id);
    let updated_receiver = alert_rx_get(client, &receiver_url).await;
    assert_eq!(updated_receiver.identity.description, "an updated description");
}

#[nexus_test]
async fn test_webhook_receiver_create_delete(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Create a webhook receiver.
    let created_webhook = webhook_create(
        &cptestctx,
        &params::WebhookCreate {
            identity: my_great_webhook_identity(),
            endpoint: "https://example.com/webhooks"
                .parse()
                .expect("this should be a valid URL"),
            secrets: vec![MY_COOL_SECRET.to_string()],
            subscriptions: vec!["test.foo".parse().unwrap()],
        },
    )
    .await;
    dbg!(&created_webhook);

    let delete_url = alert_rx_url(created_webhook.identity.name.clone());
    resource_helpers::object_delete(client, &delete_url).await;

    // It should be gone now.
    resource_helpers::object_delete_error(
        client,
        &delete_url,
        http::StatusCode::NOT_FOUND,
    )
    .await;
}

#[nexus_test]
async fn test_webhook_receiver_names_are_unique(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Create a webhook receiver.
    let created_webhook = webhook_create(
        &cptestctx,
        &params::WebhookCreate {
            identity: my_great_webhook_identity(),
            endpoint: "https://example.com/webhooks"
                .parse()
                .expect("this should be a valid URL"),
            secrets: vec![MY_COOL_SECRET.to_string()],
            subscriptions: vec!["test.foo".parse().unwrap()],
        },
    )
    .await;
    dbg!(&created_webhook);

    let error = resource_helpers::object_create_error(
        &client,
        WEBHOOK_RECEIVERS_BASE_PATH,
        &params::WebhookCreate {
            identity: my_great_webhook_identity(),
            endpoint: "https://example.com/more-webhooks"
                .parse()
                .expect("this should be a valid URL"),
            secrets: vec![MY_COOL_SECRET.to_string()],
            subscriptions: vec!["test.foo.bar".parse().unwrap()],
        },
        http::StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(
        dbg!(&error).message,
        "already exists: alert-receiver \"my-great-webhook\""
    );
}

#[nexus_test]
async fn test_cannot_subscribe_to_probes(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    let error = resource_helpers::object_create_error(
        &client,
        WEBHOOK_RECEIVERS_BASE_PATH,
        &params::WebhookCreate {
            identity: my_great_webhook_identity(),
            endpoint: "https://example.com/webhooks"
                .parse()
                .expect("this should be a valid URL"),
            secrets: vec![MY_COOL_SECRET.to_string()],
            subscriptions: vec![
                "probe".parse().unwrap(),
                "test.foo".parse().unwrap(),
            ],
        },
        http::StatusCode::BAD_REQUEST,
    )
    .await;
    assert!(
        dbg!(&error)
            .message
            .contains("webhook receivers cannot subscribe to probes"),
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

    let id = AlertUuid::new_v4();

    // Create a webhook receiver.
    let webhook =
        webhook_create(&cptestctx, &my_great_webhook_params(&server)).await;
    dbg!(&webhook);

    let mock = {
        let webhook = webhook.clone();
        server
            .mock_async(move |when, then| {
                let body = serde_json::json!({
                    "alert_class": "test.foo",
                    "alert_id": id,
                    "data": {
                        "hello_world": true,
                    }
                })
                .to_string();
                when.method(POST)
                    .header("x-oxide-alert-class", "test.foo")
                    .header("x-oxide-alert-id", id.to_string())
                    .and(is_valid_for_webhook(&webhook))
                    .is_true(signature_verifies(
                        webhook.config.secrets[0].id,
                        MY_COOL_SECRET.as_bytes().to_vec(),
                    ))
                    .json_body_includes(body);
                then.status(200);
            })
            .await
    };

    // Publish an event
    let event = nexus
        .alert_publish(
            &opctx,
            id,
            AlertClass::TestFoo,
            serde_json::json!({"hello_world": true}),
        )
        .await
        .expect("event should be published successfully");
    dbg!(event);

    dbg!(activate_background_task(internal_client, "alert_dispatcher").await);
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

    let id = AlertUuid::new_v4();
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
            subscriptions: vec!["test.foo".parse().unwrap()],
        },
    )
    .await;
    dbg!(&webhook);
    let rx_id = AlertReceiverUuid::from_untyped_uuid(webhook.identity.id);

    let secret1_id = webhook.config.secrets[0].id;

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
                .header("x-oxide-alert-class", "test.foo")
                .header("x-oxide-alert-id", id.to_string())
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
        .alert_publish(
            &opctx,
            id,
            AlertClass::TestFoo,
            serde_json::json!({"hello_world": true}),
        )
        .await
        .expect("event should be published successfully");
    dbg!(event);

    dbg!(activate_background_task(internal_client, "alert_dispatcher").await);
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

    let bar_alert_id = AlertUuid::new_v4();
    let baz_alert_id = AlertUuid::new_v4();

    let assert_webhook_rx_list_matches =
        |mut expected: Vec<views::AlertReceiver>| async move {
            let mut actual = alert_rx_list(client).await;
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
            subscriptions: vec!["test.foo.bar".parse().unwrap()],
        },
    )
    .await;
    dbg!(&rx_bar);
    assert_webhook_rx_list_matches(vec![rx_bar.clone().into()]).await;
    let mock_bar = {
        let webhook = rx_bar.clone();
        srv_bar
            .mock_async(move |when, then| {
                when.method(POST)
                    .header("x-oxide-alert-class", "test.foo.bar")
                    .header("x-oxide-alert-id", bar_alert_id.to_string())
                    .and(is_valid_for_webhook(&webhook))
                    .is_true(signature_verifies(
                        webhook.config.secrets[0].id,
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
            subscriptions: vec!["test.foo.baz".parse().unwrap()],
        },
    )
    .await;
    dbg!(&rx_baz);
    assert_webhook_rx_list_matches(vec![
        rx_bar.clone().into(),
        rx_baz.clone().into(),
    ])
    .await;
    let mock_baz = {
        let webhook = rx_baz.clone();
        srv_baz
            .mock_async(move |when, then| {
                when.method(POST)
                    .header("x-oxide-alert-class", "test.foo.baz")
                    .header("x-oxide-alert-id", baz_alert_id.to_string())
                    .and(is_valid_for_webhook(&webhook))
                    .is_true(signature_verifies(
                        webhook.config.secrets[0].id,
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
            subscriptions: vec!["test.foo.*".parse().unwrap()],
        },
    )
    .await;
    dbg!(&rx_star);
    assert_webhook_rx_list_matches(vec![
        rx_bar.clone().into(),
        rx_baz.clone().into(),
        rx_star.clone().into(),
    ])
    .await;
    let mock_star = {
        let webhook = rx_star.clone();
        srv_star
            .mock_async(move |when, then| {
                when.method(POST)
                    .header_matches(
                        "x-oxide-alert-class",
                        "test\\.foo\\.ba[rz]",
                    )
                    .header_exists("x-oxide-alert-id")
                    .and(is_valid_for_webhook(&webhook))
                    .is_true(signature_verifies(
                        webhook.config.secrets[0].id,
                        STAR_SECRET.as_bytes().to_vec(),
                    ));
                then.status(200);
            })
            .await
    };

    // Publish a test.foo.bar event
    let event = nexus
        .alert_publish(
            &opctx,
            bar_alert_id,
            AlertClass::TestFooBar,
            serde_json::json!({"lol": "webhooked on phonics"}),
        )
        .await
        .expect("event should be published successfully");
    dbg!(event);
    // Publish a test.foo.baz event
    let event = nexus
        .alert_publish(
            &opctx,
            baz_alert_id,
            AlertClass::TestFooBaz,
            serde_json::json!({"lol": "webhook, line, and sinker"}),
        )
        .await
        .expect("event should be published successfully");
    dbg!(event);

    dbg!(activate_background_task(internal_client, "alert_dispatcher").await);
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

    let id = AlertUuid::new_v4();

    // Create a webhook receiver.
    let webhook =
        webhook_create(&cptestctx, &my_great_webhook_params(&server)).await;
    dbg!(&webhook);

    let mock = {
        let webhook = webhook.clone();
        server
            .mock_async(move |when, then| {
                let body = serde_json::json!({
                    "alert_class": "test.foo",
                    "alert_id": id,
                    "data": {
                        "hello_world": true,
                    }
                })
                .to_string();
                when.method(POST)
                    .header("x-oxide-alert-class", "test.foo")
                    .header("x-oxide-alert-id", id.to_string())
                    .and(is_valid_for_webhook(&webhook))
                    .is_true(signature_verifies(
                        webhook.config.secrets[0].id,
                        MY_COOL_SECRET.as_bytes().to_vec(),
                    ))
                    .json_body_includes(body);
                then.status(500);
            })
            .await
    };

    // Publish an event
    let event = nexus
        .alert_publish(
            &opctx,
            id,
            AlertClass::TestFoo,
            serde_json::json!({"hello_world": true}),
        )
        .await
        .expect("event should be published successfully");
    dbg!(event);

    dbg!(activate_background_task(internal_client, "alert_dispatcher").await);
    dbg!(
        activate_background_task(internal_client, "webhook_deliverator").await
    );

    mock.assert_calls_async(1).await;

    let deliveries =
        alert_deliveries_list(&cptestctx.external_client, webhook.identity.id)
            .await;
    assert_eq!(
        deliveries.all_items.len(),
        1,
        "expected one delivery, but found: {:?}",
        deliveries.all_items
    );

    let delivery = dbg!(&deliveries.all_items[0]);
    assert_eq!(delivery.receiver_id.into_untyped_uuid(), webhook.identity.id);
    assert_eq!(delivery.alert_id, id);
    assert_eq!(delivery.alert_class, "test.foo");
    assert_eq!(delivery.state, views::AlertDeliveryState::Pending);
    expect_delivery_attempts(
        &delivery.attempts,
        &[ExpectAttempt {
            status: Some(500),
            result: views::WebhookDeliveryAttemptResult::FailedHttpError,
        }],
    );

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
                    "alert_class": "test.foo",
                    "alert_id": id,
                    "data": {
                        "hello_world": true,
                    }
                })
                .to_string();
                when.method(POST)
                    .header("x-oxide-alert-class", "test.foo")
                    .header("x-oxide-alert-id", id.to_string())
                    .and(is_valid_for_webhook(&webhook))
                    .is_true(signature_verifies(
                        webhook.config.secrets[0].id,
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

    let deliveries =
        alert_deliveries_list(&cptestctx.external_client, webhook.identity.id)
            .await;
    assert_eq!(
        deliveries.all_items.len(),
        1,
        "expected one delivery, but found: {:?}",
        deliveries.all_items
    );

    let delivery = dbg!(&deliveries.all_items[0]);
    assert_eq!(delivery.receiver_id.into_untyped_uuid(), webhook.identity.id);
    assert_eq!(delivery.alert_id, id);
    assert_eq!(delivery.alert_class, "test.foo");
    assert_eq!(delivery.state, views::AlertDeliveryState::Pending);
    expect_delivery_attempts(
        &delivery.attempts,
        &[
            ExpectAttempt {
                status: Some(500),
                result: views::WebhookDeliveryAttemptResult::FailedHttpError,
            },
            ExpectAttempt {
                status: Some(503),
                result: views::WebhookDeliveryAttemptResult::FailedHttpError,
            },
        ],
    );

    // Finally, allow the request to succeed.
    let mock = {
        let webhook = webhook.clone();
        server
            .mock_async(move |when, then| {
                let body = serde_json::json!({
                    "alert_class": "test.foo",
                    "alert_id": id,
                    "data": {
                        "hello_world": true,
                    }
                })
                .to_string();
                when.method(POST)
                    .header("x-oxide-alert-class", "test.foo")
                    .header("x-oxide-alert-id", id.to_string())
                    .and(is_valid_for_webhook(&webhook))
                    .is_true(signature_verifies(
                        webhook.config.secrets[0].id,
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

    // Make sure the deliveries endpoint correctly records the request history.
    let deliveries =
        alert_deliveries_list(&cptestctx.external_client, webhook.identity.id)
            .await;
    assert_eq!(
        deliveries.all_items.len(),
        1,
        "expected one delivery, but found: {:?}",
        deliveries.all_items
    );
    let delivery = dbg!(&deliveries.all_items[0]);
    assert_eq!(delivery.receiver_id.into_untyped_uuid(), webhook.identity.id);
    assert_eq!(delivery.alert_id, id);
    assert_eq!(delivery.alert_class, "test.foo");
    assert_eq!(delivery.state, views::AlertDeliveryState::Delivered);
    expect_delivery_attempts(
        &delivery.attempts,
        &[
            ExpectAttempt {
                status: Some(500),
                result: views::WebhookDeliveryAttemptResult::FailedHttpError,
            },
            ExpectAttempt {
                status: Some(503),
                result: views::WebhookDeliveryAttemptResult::FailedHttpError,
            },
            ExpectAttempt {
                status: Some(200),
                result: views::WebhookDeliveryAttemptResult::Succeeded,
            },
        ],
    );
}

#[nexus_test]
async fn test_probe(cptestctx: &ControlPlaneTestContext) {
    let server = httpmock::MockServer::start_async().await;

    // Create a webhook receiver.
    let webhook =
        webhook_create(&cptestctx, &my_great_webhook_params(&server)).await;
    dbg!(&webhook);
    let rx_id = AlertReceiverUuid::from_untyped_uuid(webhook.identity.id);

    let body = serde_json::json!({
        "alert_class": "probe",
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
                    .header("x-oxide-alert-class", "probe")
                    .and(is_valid_for_webhook(&webhook))
                    .is_true(signature_verifies(
                        webhook.config.secrets[0].id,
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
    let probe1 = alert_receiver_send_probe(
        &cptestctx,
        &rx_id,
        false,
        http::StatusCode::OK,
    )
    .await;
    dbg!(&probe1);

    mock.assert_async().await;

    expect_delivery_attempts(
        &probe1.probe.attempts,
        &[ExpectAttempt {
            result: views::WebhookDeliveryAttemptResult::FailedTimeout,
            status: None,
        }],
    );
    assert_eq!(probe1.probe.alert_class, "probe");
    assert_eq!(probe1.probe.trigger, views::AlertDeliveryTrigger::Probe);
    assert_eq!(probe1.probe.state, views::AlertDeliveryState::Failed);
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
                    .header("x-oxide-alert-class", "probe")
                    .and(is_valid_for_webhook(&webhook))
                    .is_true(signature_verifies(
                        webhook.config.secrets[0].id,
                        MY_COOL_SECRET.as_bytes().to_vec(),
                    ))
                    .json_body_includes(body);
                then.status(503);
            })
            .await
    };

    let probe2 = alert_receiver_send_probe(
        &cptestctx,
        &rx_id,
        false,
        http::StatusCode::OK,
    )
    .await;
    dbg!(&probe2);

    mock.assert_async().await;
    expect_delivery_attempts(
        &probe2.probe.attempts,
        &[ExpectAttempt {
            result: views::WebhookDeliveryAttemptResult::FailedHttpError,
            status: Some(503),
        }],
    );
    assert_eq!(probe2.probe.alert_class, "probe");
    assert_eq!(probe2.probe.trigger, views::AlertDeliveryTrigger::Probe);
    assert_eq!(probe2.probe.state, views::AlertDeliveryState::Failed);
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
                    .header("x-oxide-alert-class", "probe")
                    .and(is_valid_for_webhook(&webhook))
                    .is_true(signature_verifies(
                        webhook.config.secrets[0].id,
                        MY_COOL_SECRET.as_bytes().to_vec(),
                    ))
                    .json_body_includes(body);
                then.status(200);
            })
            .await
    };

    let probe3 = alert_receiver_send_probe(
        &cptestctx,
        &rx_id,
        false,
        http::StatusCode::OK,
    )
    .await;
    dbg!(&probe3);
    mock.assert_async().await;

    expect_delivery_attempts(
        &probe3.probe.attempts,
        &[ExpectAttempt {
            result: views::WebhookDeliveryAttemptResult::Succeeded,
            status: Some(200),
        }],
    );
    assert_eq!(probe3.probe.alert_class, "probe");
    assert_eq!(probe3.probe.trigger, views::AlertDeliveryTrigger::Probe);
    assert_eq!(probe3.probe.state, views::AlertDeliveryState::Delivered);
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

    let event1_id = AlertUuid::new_v4();
    let event2_id = AlertUuid::new_v4();
    let mock = {
        let webhook = webhook.clone();
        server
            .mock_async(move |when, then| {
                when.method(POST)
                    .header("x-oxide-alert-class", "test.foo")
                    // either event
                    .header_matches(
                        "x-oxide-alert-id",
                        format!("({event1_id})|({event2_id})"),
                    )
                    .and(is_valid_for_webhook(&webhook))
                    .is_true(signature_verifies(
                        webhook.config.secrets[0].id,
                        MY_COOL_SECRET.as_bytes().to_vec(),
                    ));
                then.status(500);
            })
            .await
    };

    // Publish both events
    dbg!(
        nexus
            .alert_publish(
                &opctx,
                event1_id,
                AlertClass::TestFoo,
                serde_json::json!({"hello": "world"}),
            )
            .await
            .expect("event1 should be published successfully")
    );
    dbg!(
        nexus
            .alert_publish(
                &opctx,
                event2_id,
                AlertClass::TestFoo,
                serde_json::json!({"hello": "emeryville"}),
            )
            .await
            .expect("event2 should be published successfully")
    );

    dbg!(activate_background_task(internal_client, "alert_dispatcher").await);
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
                    "alert_class": "probe",
                    "data": {
                    }
                })
                .to_string();
                when.method(POST)
                    .header("x-oxide-alert-class", "probe")
                    .and(is_valid_for_webhook(&webhook))
                    .is_true(signature_verifies(
                        webhook.config.secrets[0].id,
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
                    .header("x-oxide-alert-class", "test.foo")
                    // either event
                    .header_matches(
                        "x-oxide-alert-id",
                        format!("({event1_id})|({event2_id})"),
                    )
                    .and(is_valid_for_webhook(&webhook))
                    .is_true(signature_verifies(
                        webhook.config.secrets[0].id,
                        MY_COOL_SECRET.as_bytes().to_vec(),
                    ));
                then.status(200);
            })
            .await
    };

    // Send a probe with ?resend=true
    let rx_id = AlertReceiverUuid::from_untyped_uuid(webhook.identity.id);
    let probe = alert_receiver_send_probe(
        &cptestctx,
        &rx_id,
        true,
        http::StatusCode::OK,
    )
    .await;
    dbg!(&probe);
    probe_mock.assert_async().await;
    probe_mock.delete_async().await;
    assert_eq!(probe.probe.state, views::AlertDeliveryState::Delivered);
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

    let event1_id = AlertUuid::new_v4();
    let event2_id = AlertUuid::new_v4();
    let body = serde_json::json!({
        "alert_class": "test.foo",
        "alert_id": event1_id,
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
                    .header("x-oxide-alert-class", "test.foo")
                    .header("x-oxide-alert-id", event1_id.to_string())
                    .and(is_valid_for_webhook(&webhook))
                    .is_true(signature_verifies(
                        webhook.config.secrets[0].id,
                        MY_COOL_SECRET.as_bytes().to_vec(),
                    ))
                    .json_body_includes(body);
                then.status(500);
            })
            .await
    };

    // Publish an event
    let event1 = nexus
        .alert_publish(
            &opctx,
            event1_id,
            AlertClass::TestFoo,
            serde_json::json!({"hello_world": true}),
        )
        .await
        .expect("event should be published successfully");
    dbg!(event1);

    // Publish another event that our receiver is not subscribed to.
    let event2 = nexus
        .alert_publish(
            &opctx,
            event2_id,
            AlertClass::TestQuuxBar,
            serde_json::json!({"hello_world": true}),
        )
        .await
        .expect("event should be published successfully");
    dbg!(event2);

    dbg!(activate_background_task(internal_client, "alert_dispatcher").await);
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
                    .header("x-oxide-alert-class", "test.foo")
                    .header("x-oxide-alert-id", event1_id.to_string())
                    .and(is_valid_for_webhook(&webhook))
                    .is_true(signature_verifies(
                        webhook.config.secrets[0].id,
                        MY_COOL_SECRET.as_bytes().to_vec(),
                    ))
                    .json_body_includes(body);
                then.status(200);
            })
            .await
    };

    // Try to resend event 1.
    let delivery =
        alert_delivery_resend(client, webhook.identity.id, event1_id).await;
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

#[nexus_test]
async fn test_subscription_add(cptestctx: &ControlPlaneTestContext) {
    subscription_add_test(cptestctx, "test.foo.bar").await
}

#[nexus_test]
async fn test_glob_subscription_add(cptestctx: &ControlPlaneTestContext) {
    subscription_add_test(cptestctx, "test.foo.*").await
}

async fn subscription_add_test(
    cptestctx: &ControlPlaneTestContext,
    new_subscription: &str,
) {
    let nexus = cptestctx.server.server_context().nexus.clone();
    let internal_client = &cptestctx.internal_client;

    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    let server = httpmock::MockServer::start_async().await;

    let id1 = AlertUuid::new_v4();
    let id2 = AlertUuid::new_v4();

    // Create a webhook receiver.
    let webhook =
        webhook_create(&cptestctx, &my_great_webhook_params(&server)).await;
    dbg!(&webhook);

    let mock = {
        let webhook = webhook.clone();
        server
            .mock_async(move |when, then| {
                let body = serde_json::json!({
                    "alert_class": "test.foo.bar",
                    "alert_id": id2,
                    "data": {
                        "hello_world": true,
                    }
                })
                .to_string();
                when.method(POST)
                    .header("x-oxide-alert-class", "test.foo.bar")
                    .header("x-oxide-alert-id", id2.to_string())
                    .and(is_valid_for_webhook(&webhook))
                    .is_true(signature_verifies(
                        webhook.config.secrets[0].id,
                        MY_COOL_SECRET.as_bytes().to_vec(),
                    ))
                    .json_body_includes(body);
                then.status(200);
            })
            .await
    };

    // Publish an event. This should not be received, as we are not subscribed
    // to it.
    let event = nexus
        .alert_publish(
            &opctx,
            id1,
            AlertClass::TestFooBar,
            serde_json::json!({"hello_world": false}),
        )
        .await
        .expect("event should be published successfully");
    dbg!(event);

    dbg!(activate_background_task(internal_client, "alert_dispatcher").await);
    dbg!(
        activate_background_task(internal_client, "webhook_deliverator").await
    );

    mock.assert_calls_async(0).await;

    let rx_id = AlertReceiverUuid::from_untyped_uuid(webhook.identity.id);
    let new_subscription =
        new_subscription.parse::<shared::AlertSubscription>().unwrap();
    dbg!(subscription_add(&cptestctx, rx_id, &new_subscription).await);

    // The new subscription should be there.
    let rx = alert_rx_get(
        &cptestctx.external_client,
        &alert_rx_url(webhook.identity.id),
    )
    .await;
    dbg!(&rx);
    assert!(rx.subscriptions.contains(&new_subscription));

    // Publish an event. This one should make it through.
    let event = nexus
        .alert_publish(
            &opctx,
            id2,
            AlertClass::TestFooBar,
            serde_json::json!({"hello_world": true}),
        )
        .await
        .expect("event should be published successfully");
    dbg!(event);

    dbg!(activate_background_task(internal_client, "alert_dispatcher").await);
    dbg!(
        activate_background_task(internal_client, "webhook_deliverator").await
    );

    mock.assert_calls_async(1).await;

    // Finally, make sure adding a subscription is idempotent. A second POST
    // should succeed, even though the subscriptions already exist.
    let original_subscription = "test.foo".parse().unwrap();
    dbg!(subscription_add(&cptestctx, rx_id, &new_subscription).await);
    dbg!(subscription_add(&cptestctx, rx_id, &original_subscription).await);
}

#[nexus_test]
async fn test_subscription_remove(cptestctx: &ControlPlaneTestContext) {
    subscription_remove_test(cptestctx, "test.foo.bar").await
}

#[nexus_test]
async fn test_subscription_remove_glob(cptestctx: &ControlPlaneTestContext) {
    subscription_remove_test(cptestctx, "test.foo.*").await
}

async fn subscription_remove_test(
    cptestctx: &ControlPlaneTestContext,
    deleted_subscription: &str,
) {
    let nexus = cptestctx.server.server_context().nexus.clone();
    let internal_client = &cptestctx.internal_client;

    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    let server = httpmock::MockServer::start_async().await;

    let id1 = AlertUuid::new_v4();
    let id2 = AlertUuid::new_v4();
    let id3 = AlertUuid::new_v4();

    let other_subscription =
        "test.foo".parse::<shared::AlertSubscription>().unwrap();
    let deleted_subscription =
        deleted_subscription.parse::<shared::AlertSubscription>().unwrap();

    // Create a webhook receiver.
    let webhook = webhook_create(
        &cptestctx,
        &params::WebhookCreate {
            subscriptions: vec![
                other_subscription.clone(),
                deleted_subscription.clone(),
            ],
            ..my_great_webhook_params(&server)
        },
    )
    .await;
    dbg!(&webhook);

    let mock = {
        let webhook = webhook.clone();
        server
            .mock_async(move |when, then| {
                let body = serde_json::json!({
                    "alert_class": "test.foo.bar",
                    "alert_id": id1,
                    "data": {
                        "hello_world": true,
                    }
                })
                .to_string();
                when.method(POST)
                    .header("x-oxide-alert-class", "test.foo.bar")
                    .header("x-oxide-alert-id", id1.to_string())
                    .and(is_valid_for_webhook(&webhook))
                    .is_true(signature_verifies(
                        webhook.config.secrets[0].id,
                        MY_COOL_SECRET.as_bytes().to_vec(),
                    ))
                    .json_body_includes(body);
                then.status(200);
            })
            .await
    };

    // Publish an event. This should be received, as it matches the subscription
    // we are about to delete.
    let event = nexus
        .alert_publish(
            &opctx,
            id1,
            AlertClass::TestFooBar,
            serde_json::json!({"hello_world": true}),
        )
        .await
        .expect("event should be published successfully");
    dbg!(event);

    dbg!(activate_background_task(internal_client, "alert_dispatcher").await);
    dbg!(
        activate_background_task(internal_client, "webhook_deliverator").await
    );

    mock.assert_calls_async(1).await;

    let rx_id = AlertReceiverUuid::from_untyped_uuid(webhook.identity.id);
    dbg!(subscription_remove(&cptestctx, rx_id, &deleted_subscription).await);

    // The deleted subscription should no longer be there.
    let rx = alert_rx_get(
        &cptestctx.external_client,
        &alert_rx_url(webhook.identity.id),
    )
    .await;
    dbg!(&rx);
    assert_eq!(rx.subscriptions, vec![other_subscription.clone()]);

    // Publish an event. This one should not be received, as we are no longer
    // subscribed to its event class.
    let event = nexus
        .alert_publish(
            &opctx,
            id2,
            AlertClass::TestFooBar,
            serde_json::json!({"hello_world": false}),
        )
        .await
        .expect("event should be published successfully");
    dbg!(event);

    dbg!(activate_background_task(internal_client, "alert_dispatcher").await);
    dbg!(
        activate_background_task(internal_client, "webhook_deliverator").await
    );

    // No new calls should be observed.
    mock.assert_calls_async(1).await;

    // Finally, ensure that the other event class we were subscribed to still
    // goes through.
    let mock = {
        let webhook = webhook.clone();
        server
            .mock_async(move |when, then| {
                let body = serde_json::json!({
                    "alert_class": "test.foo",
                    "alert_id": id3,
                    "data": {
                        "whatever": 1
                    }
                })
                .to_string();
                when.method(POST)
                    .header("x-oxide-alert-class", "test.foo")
                    .header("x-oxide-alert-id", id3.to_string())
                    .and(is_valid_for_webhook(&webhook))
                    .is_true(signature_verifies(
                        webhook.config.secrets[0].id,
                        MY_COOL_SECRET.as_bytes().to_vec(),
                    ))
                    .json_body_includes(body);
                then.status(200);
            })
            .await
    };

    let event = nexus
        .alert_publish(
            &opctx,
            id3,
            AlertClass::TestFoo,
            serde_json::json!({"whatever": 1}),
        )
        .await
        .expect("event should be published successfully");
    dbg!(event);

    dbg!(activate_background_task(internal_client, "alert_dispatcher").await);
    dbg!(
        activate_background_task(internal_client, "webhook_deliverator").await
    );

    mock.assert_calls_async(1).await;

    // Deleting a subscription that doesn't exist should 404.
    dbg!(
        resource_helpers::object_delete_error(
            &internal_client,
            &subscription_remove_url(rx_id, &deleted_subscription),
            http::StatusCode::NOT_FOUND
        )
        .await
    );
}
