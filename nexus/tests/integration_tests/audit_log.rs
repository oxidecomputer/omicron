// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::{DateTime, Utc};
use dropshot::{ResultsPage, test_util::ClientTestContext};
use http::{Method, StatusCode, header};
use nexus_db_queries::authn::USER_TEST_PRIVILEGED;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::resource_helpers::{
    create_console_session, create_local_user, create_project, create_silo,
    object_create_error, objects_list_page_authz, test_params,
};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::{params, shared, views};
use nexus_types::{identity::Asset, silo::DEFAULT_SILO_ID};
use omicron_common::api::external::{
    IdentityMetadataCreateParams, Name, UserId,
};
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

    // we have to do this rigmarole for this request in order to get the
    // user agent header in there and to use a session cookie to test the
    // access_method field

    // this this creates its own entry
    let session_cookie = create_console_session(ctx).await;

    let t3 = Utc::now(); // after second entry

    let body = &params::ProjectCreate {
        identity: IdentityMetadataCreateParams {
            name: "test-proj2".parse().unwrap(),
            description: "a pier".to_string(),
        },
    };
    RequestBuilder::new(client, Method::POST, "/v1/projects")
        .body(Some(&body))
        .header(header::COOKIE, session_cookie.clone())
        .header("User-Agent", "A pretend user agent string")
        .expect_status(Some(StatusCode::CREATED))
        .execute()
        .await
        .expect("failed to 201 on project create with session");

    let t4 = Utc::now(); // third entry

    let audit_log = fetch_log(client, t1, None).await;
    assert_eq!(audit_log.items.len(), 3);

    let e1 = &audit_log.items[0];
    let e2 = &audit_log.items[1];
    let e3 = &audit_log.items[2];

    assert_eq!(e1.request_uri, "/v1/projects");
    assert_eq!(e1.operation_id, "project_create");
    assert_eq!(e1.source_ip.to_string(), "127.0.0.1");
    assert_eq!(e1.user_agent, None); // no user agent passed by default
    assert_eq!(e1.access_method, Some("spoof".to_string()));
    assert!(e1.time_started >= t1 && e1.time_started <= t2);
    assert!(e1.time_completed > e1.time_started);
    assert_eq!(
        e1.actor,
        views::AuditLogEntryActor::SiloUser {
            silo_user_id: USER_TEST_PRIVILEGED.id(),
            silo_id: DEFAULT_SILO_ID,
        }
    );

    // second was the login attempt
    assert_eq!(e2.request_uri, "/v1/login/test-suite-silo/local");
    assert_eq!(e2.operation_id, "login_local");
    assert_eq!(e2.source_ip.to_string(), "127.0.0.1");
    assert_eq!(e2.user_agent, None); // no user agent passed by default
    assert_eq!(e2.access_method, None);
    assert!(e2.time_started >= t2 && e2.time_started <= t3);
    assert!(e2.time_completed > e2.time_started);

    // login attempts are unauthenticated (until the user is authenticated)
    // assert_eq!(e2.actor, views::AuditLogEntryActor::Unauthenticated);

    // TODO: because we are using the opctx to determine the actor, the actor
    // here is the built in external authenticator user. This is misleading.
    // I need to change this in the logging code, maybe with a special init
    // function for unauthenticated endpoints that doesn't pull the actor out
    // of the opctx, instead taking a potential actor like the username.
    assert_eq!(
        e2.actor,
        views::AuditLogEntryActor::UserBuiltin {
            user_builtin_id: "001de000-05e4-4000-8000-000000000003"
                .parse()
                .unwrap()
        }
    );

    // session create was the test suite user in the test suite silo, which
    // is different from the privileged user, so we need to fetch the user
    // and silo ID using the session to check them against the audit log
    let me = RequestBuilder::new(client, Method::GET, "/v1/me")
        .header(header::COOKIE, session_cookie)
        .expect_status(Some(StatusCode::OK))
        .execute()
        .await
        .expect("failed to 201 on project create with session")
        .parsed_body::<views::CurrentUser>()
        .unwrap();

    // third one was done with the session cookie, reflected in access_method
    assert_eq!(e3.request_uri, "/v1/projects");
    assert_eq!(e3.operation_id, "project_create");
    assert_eq!(e3.source_ip.to_string(), "127.0.0.1");
    assert_eq!(e3.user_agent.as_ref().unwrap(), "A pretend user agent string");
    assert_eq!(e3.access_method, Some("session_cookie".to_string()));
    assert!(e3.time_started >= t3 && e3.time_started <= t4);
    assert!(e3.time_completed > e3.time_started);
    assert_eq!(
        e3.actor,
        views::AuditLogEntryActor::SiloUser {
            silo_user_id: me.user.id,
            silo_id: me.user.silo_id,
        }
    );

    // we can exclude the entries by timestamp
    let audit_log = fetch_log(client, t2, Some(t2)).await;
    assert_eq!(audit_log.items.len(), 0);

    let audit_log = fetch_log(client, t2, None).await;
    assert_eq!(audit_log.items.len(), 2);

    let audit_log = fetch_log(client, t3, None).await;
    assert_eq!(audit_log.items.len(), 1);

    // TODO: think about whether requiring start time and having end time be
    // optional (and later than it) still makes sense when order is reversed

    // test reverse order
    let url = format!(
        "/v1/system/audit-log?sort_by=time_and_id_descending&start_time={}",
        to_q(t1)
    );
    let reverse_log =
        objects_list_page_authz::<views::AuditLogEntry>(client, &url).await;
    assert_eq!(reverse_log.items.len(), 3);
    assert_eq!(e1.id, reverse_log.items[2].id);
    assert_eq!(e2.id, reverse_log.items[1].id);
    assert_eq!(e3.id, reverse_log.items[0].id);

    // test pagination cursor. with limit 1, we only get one item, and it's e1
    let url = format!("/v1/system/audit-log?start_time={}&limit=1", to_q(t1));
    let log =
        objects_list_page_authz::<views::AuditLogEntry>(client, &url).await;
    assert_eq!(log.items.len(), 1);
    assert_eq!(e1.id, log.items[0].id);

    // when we use the next page cursor we should get e2
    let url = format!(
        "/v1/system/audit-log?page_token={}&limit=1",
        log.next_page.unwrap()
    );
    let log =
        objects_list_page_authz::<views::AuditLogEntry>(client, &url).await;
    assert_eq!(log.items.len(), 1);
    assert_eq!(e2.id, log.items[0].id);
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
    let params = test_params::UserPassword::Password("correct-password".into());
    create_local_user(client, &silo, &test_user, params).await;

    let t1 = Utc::now(); // before log entry

    // Failed login attempt should create audit log entry
    let silo_name = Name::from_str("test-silo").unwrap();
    expect_login_failure(client, &silo_name, &test_user, "wrong-password")
        .await;

    let t2 = Utc::now(); // after first entry

    // Successful login attempt should create audit log entry
    expect_login_success(client, &silo_name, &test_user, "correct-password")
        .await;

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
    assert_eq!(e1.error_code, Some("Unauthorized".to_string()));
    assert_eq!(
        e1.error_message,
        Some("credentials missing or invalid".to_string())
    );
    assert!(e1.time_started >= t1 && e1.time_started <= t2);
    assert!(e1.time_completed > e1.time_started);

    // Verify second entry (successful login)
    assert_eq!(e2.request_uri, "/v1/login/test-silo/local");
    assert_eq!(e2.operation_id, "login_local");
    assert_eq!(e2.source_ip.to_string(), "127.0.0.1");
    assert_eq!(e2.http_status_code, 204);
    assert_eq!(e2.error_code, None);
    assert_eq!(e2.error_message, None);
    assert!(e2.time_started >= t2 && e2.time_started <= t3);
    assert!(e2.time_completed > e2.time_started);

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
    password: &str,
) {
    let login_url = format!("/v1/login/{}/local", silo_name);
    let params = &test_params::UsernamePasswordCredentials {
        username: username.clone(),
        password: password.into(),
    };
    let status = StatusCode::UNAUTHORIZED;
    let error = object_create_error(client, &login_url, params, status).await;
    assert_eq!(error.message, "credentials missing or invalid");
}

async fn expect_login_success(
    client: &ClientTestContext,
    silo_name: &Name,
    username: &UserId,
    password: &str,
) {
    let login_url = format!("/v1/login/{}/local", silo_name);
    RequestBuilder::new(client, Method::POST, &login_url)
        .body(Some(&test_params::UsernamePasswordCredentials {
            username: username.clone(),
            password: password.into(),
        }))
        .expect_status(Some(StatusCode::NO_CONTENT))
        .execute()
        .await
        .expect("expected successful login, but it failed");
}
