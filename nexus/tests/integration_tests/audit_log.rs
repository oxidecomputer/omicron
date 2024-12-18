// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::{DateTime, Utc};
use dropshot::{ResultsPage, test_util::ClientTestContext};
use http::{Method, StatusCode};
use nexus_db_queries::authn::USER_TEST_PRIVILEGED;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::resource_helpers::{
    create_local_user, create_project, create_silo, object_create_error,
    objects_list_page_authz,
};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::{params, shared, views};
use nexus_types::{identity::Asset, silo::DEFAULT_SILO_ID};
use omicron_common::api::external::{Name, UserId};
use std::str::FromStr;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

fn to_q(d: DateTime<Utc>) -> String {
    d.to_rfc3339_opts(chrono::SecondsFormat::Micros, true)
}

async fn fetch_log(
    client: &ClientTestContext,
    start: DateTime<Utc>,
    end: Option<DateTime<Utc>>,
) -> ResultsPage<views::AuditLogEntry> {
    let mut qs = vec![format!("start_time={}", to_q(start))];
    if let Some(end) = end {
        qs.push(format!("end_time={}", to_q(end)));
    }
    let url = format!("/v1/system/audit-log?{}", qs.join("&"));
    objects_list_page_authz::<views::AuditLogEntry>(client, &url).await
}

#[nexus_test]
async fn test_audit_log_list(ctx: &ControlPlaneTestContext) {
    let client = &ctx.external_client;

    let t0: DateTime<Utc> = "2024-01-01T00:00:00Z".parse().unwrap();
    // let t_future: DateTime<Utc> = "2099-01-01T00:00:00Z".parse().unwrap();

    let audit_log = fetch_log(client, t0, None).await;
    assert_eq!(audit_log.items.len(), 0);

    let t1 = Utc::now(); // before log entry

    // this endpoint has audit log calls in it
    create_project(client, "test-proj").await;

    let t2 = Utc::now(); // after log entry

    let audit_log = fetch_log(client, t0, None).await;
    assert_eq!(audit_log.items.len(), 1);

    // this endpoint has audit log calls in it
    create_project(client, "test-proj2").await;

    let t3 = Utc::now(); // after second entry

    let audit_log = dbg!(fetch_log(client, t1, None).await);
    assert_eq!(audit_log.items.len(), 2);

    let e1 = &audit_log.items[0];
    let e2 = &audit_log.items[1];

    assert_eq!(e1.request_uri, "/v1/projects");
    assert_eq!(e1.operation_id, "project_create");
    assert_eq!(e1.source_ip.to_string(), "127.0.0.1");
    // TODO: would be nice to test a request with a different method
    assert_eq!(e1.access_method, Some("spoof".to_string()));
    assert!(e1.timestamp >= t1 && e1.timestamp <= t2);
    assert!(e1.time_completed > e1.timestamp);
    assert_eq!(e1.actor_id, Some(USER_TEST_PRIVILEGED.id()));
    assert_eq!(e1.actor_silo_id, Some(DEFAULT_SILO_ID));

    assert_eq!(e2.request_uri, "/v1/projects");
    assert_eq!(e2.operation_id, "project_create");
    assert_eq!(e2.source_ip.to_string(), "127.0.0.1");
    assert_eq!(e2.access_method, Some("spoof".to_string()));
    assert!(e2.timestamp >= t2 && e2.timestamp <= t3);
    assert!(e2.time_completed > e2.timestamp);
    assert_eq!(e2.actor_id, Some(USER_TEST_PRIVILEGED.id()));
    assert_eq!(e2.actor_silo_id, Some(DEFAULT_SILO_ID));

    // we can exclude the entry by timestamp
    let audit_log = fetch_log(client, t2, Some(t2)).await;
    assert_eq!(audit_log.items.len(), 0);

    let audit_log = fetch_log(client, t2, None).await;
    assert_eq!(audit_log.items.len(), 1);

    // TODO: assert about list order
    // TODO: test pagination cursor
}

#[nexus_test]
async fn test_audit_log_login_local(ctx: &ControlPlaneTestContext) {
    let client = &ctx.external_client;
    let t0: DateTime<Utc> = "2024-01-01T00:00:00Z".parse().unwrap();

    let audit_log = fetch_log(client, t0, None).await;
    assert_eq!(audit_log.items.len(), 0);

    // Create test silo and user first
    let silo_name = Name::from_str("test-silo").unwrap();
    let local = shared::SiloIdentityMode::LocalOnly;
    let silo = create_silo(client, silo_name.as_str(), true, local).await;

    let test_user = UserId::from_str("test-user").unwrap();
    let test_password = params::Password::from_str("correct-password").unwrap();
    let params = params::UserPassword::Password(test_password.clone());
    create_local_user(client, &silo, &test_user, params).await;

    let t1 = Utc::now(); // before log entry

    // Failed login attempt should create audit log entry
    let silo_name = Name::from_str("test-silo").unwrap();
    let bad_password = params::Password::from_str("wrong-password").unwrap();
    expect_login_failure(client, &silo_name, &test_user, bad_password).await;

    let t2 = Utc::now(); // after first entry

    // Successful login attempt should create audit log entry
    let good_password = params::Password::from_str("correct-password").unwrap();
    expect_login_success(client, &silo_name, &test_user, good_password).await;

    let t3 = Utc::now(); // after second entry

    let audit_log = fetch_log(client, t1, None).await;
    assert_eq!(audit_log.items.len(), 2);

    let e1 = &audit_log.items[0];
    let e2 = &audit_log.items[1];

    // Verify first entry (failed login)
    assert_eq!(e1.request_uri, "/v1/login/test-silo/local");
    assert_eq!(e1.operation_id, "login_local");
    assert_eq!(e1.source_ip.to_string(), "127.0.0.1");
    assert_eq!(e1.http_status_code, 401);
    assert!(e1.timestamp >= t1 && e1.timestamp <= t2);
    assert!(e1.time_completed > e1.timestamp);

    // Verify second entry (successful login)
    assert_eq!(e2.request_uri, "/v1/login/test-silo/local");
    assert_eq!(e2.operation_id, "login_local");
    assert_eq!(e2.source_ip.to_string(), "127.0.0.1");
    assert_eq!(e2.http_status_code, 204);
    assert!(e2.timestamp >= t2 && e2.timestamp <= t3);
    assert!(e2.time_completed > e2.timestamp);

    // Time filtering works
    let audit_log = fetch_log(client, t2, Some(t2)).await;
    assert_eq!(audit_log.items.len(), 0);

    let audit_log = fetch_log(client, t2, None).await;
    assert_eq!(audit_log.items.len(), 1);
}

async fn expect_login_failure(
    client: &ClientTestContext,
    silo_name: &Name,
    username: &UserId,
    password: params::Password,
) {
    let login_url = format!("/v1/login/{}/local", silo_name);
    let params = &params::UsernamePasswordCredentials {
        username: username.clone(),
        password,
    };
    let status = StatusCode::UNAUTHORIZED;
    let error = object_create_error(client, &login_url, params, status).await;
    assert_eq!(error.message, "credentials missing or invalid");
}

async fn expect_login_success(
    client: &ClientTestContext,
    silo_name: &Name,
    username: &UserId,
    password: params::Password,
) {
    let login_url = format!("/v1/login/{}/local", silo_name);
    RequestBuilder::new(client, Method::POST, &login_url)
        .body(Some(&params::UsernamePasswordCredentials {
            username: username.clone(),
            password,
        }))
        .expect_status(Some(StatusCode::NO_CONTENT))
        .execute()
        .await
        .expect("expected successful login, but it failed");
}
