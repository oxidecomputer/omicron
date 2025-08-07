// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::{DateTime, Utc};
use dropshot::{ResultsPage, test_util::ClientTestContext};
use http::{Method, StatusCode, header};
use nexus_db_queries::authn::USER_TEST_PRIVILEGED;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::resource_helpers::{
    DiskTest, create_console_session, create_default_ip_pool, create_disk,
    create_instance_with, create_local_user, create_project, create_silo,
    object_create_error, object_delete, objects_list_page_authz, test_params,
};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::{params, shared, views};
use nexus_types::{identity::Asset, silo::DEFAULT_SILO_ID};
use omicron_common::api::external::{
    IdentityMetadataCreateParams, InstanceAutoRestartPolicy, Name, UserId,
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

    let audit_log = fetch_log(client, t0, None).await;
    assert_eq!(audit_log.items.len(), 0);

    let t1 = Utc::now(); // before log entry

    // this endpoint has audit log calls in it
    create_project(client, "test-proj").await;

    let t2 = Utc::now(); // after log entry

    let audit_log = fetch_log(client, t0, None).await;
    assert_eq!(audit_log.items.len(), 1);

    // this this creates its own entry
    let session_cookie = create_console_session(ctx).await;

    let t3 = Utc::now(); // after second entry

    // we have to do this rigmarole instead of using create_project in order to
    // get the user agent header in there and to use a session cookie to test
    // the auth_method field
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
    assert_eq!(e1.auth_method, Some("spoof".to_string()));
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
    assert_eq!(e2.auth_method, None);
    assert!(e2.time_started >= t2 && e2.time_started <= t3);
    assert!(e2.time_completed > e2.time_started);

    // login attempts are unauthenticated (until the user is authenticated)
    // assert_eq!(e2.actor, views::AuditLogEntryActor::Unauthenticated);
    assert_eq!(e2.actor, views::AuditLogEntryActor::Unauthenticated);

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

    // third one was done with the session cookie, reflected in auth_method
    assert_eq!(e3.request_uri, "/v1/projects");
    assert_eq!(e3.operation_id, "project_create");
    assert_eq!(e3.source_ip.to_string(), "127.0.0.1");
    assert_eq!(e3.user_agent.as_ref().unwrap(), "A pretend user agent string");
    assert_eq!(e3.auth_method, Some("session_cookie".to_string()));
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
    assert_eq!(
        e1.result,
        views::AuditLogEntryResult::Error {
            http_status_code: 401,
            error_code: Some("Unauthorized".to_string()),
            error_message: "credentials missing or invalid".to_string(),
        }
    );
    assert!(e1.time_started >= t1 && e1.time_started <= t2);
    assert!(e1.time_completed > e1.time_started);

    // Verify second entry (successful login)
    assert_eq!(e2.request_uri, "/v1/login/test-silo/local");
    assert_eq!(e2.operation_id, "login_local");
    assert_eq!(e2.source_ip.to_string(), "127.0.0.1");
    assert_eq!(
        e2.result,
        views::AuditLogEntryResult::Success { http_status_code: 204 }
    );
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

#[nexus_test]
async fn test_audit_log_create_delete_ops(ctx: &ControlPlaneTestContext) {
    let client = &ctx.external_client;
    let t0: DateTime<Utc> = "2024-01-01T00:00:00Z".parse().unwrap();

    // Ensure we start with an empty audit log
    let init_log = fetch_log(client, t0, None).await;
    assert_eq!(init_log.items.len(), 0);

    let t1 = Utc::now();

    // Set up disk test infrastructure and create resources with audit logging
    DiskTest::new(&ctx).await;
    create_default_ip_pool(client).await;
    let _project = create_project(client, "test-project").await;
    let _instance = create_instance_with(
        client,
        "test-project",
        "test-instance",
        &params::InstanceNetworkInterfaceAttachment::Default,
        Vec::<params::InstanceDiskAttachment>::new(),
        Vec::<params::ExternalIpCreate>::new(),
        false, // start=false, so instance is created in stopped state
        None::<InstanceAutoRestartPolicy>,
    )
    .await;
    let _disk = create_disk(client, "test-project", "test-disk").await;

    let t2 = Utc::now();

    // Extract delete URLs to variables
    let instance_del_url = "/v1/instances/test-instance?project=test-project";
    let disk_del_url = "/v1/disks/test-disk?project=test-project";
    let subnet_delete_url =
        "/v1/vpc-subnets/default?project=test-project&vpc=default";
    let vpc_delete_url = "/v1/vpcs/default?project=test-project";
    let project_del_url = "/v1/projects/test-project";

    // Delete instance, disk, default subnet, default VPC, and project
    object_delete(client, instance_del_url).await;
    object_delete(client, disk_del_url).await;
    object_delete(client, subnet_delete_url).await;
    object_delete(client, vpc_delete_url).await;
    object_delete(client, project_del_url).await;

    let t3 = Utc::now();

    // Fetch and verify all audit log entries in a single call
    let audit_log = fetch_log(client, t0, None).await;
    assert_eq!(audit_log.items.len(), 6);

    let items = &audit_log.items;

    // Verify create entries
    verify_entry(&items[0], "project_create", "/v1/projects", 201, t1, t2);
    let instances_url = "/v1/instances?project=test-project";
    verify_entry(&items[1], "instance_create", instances_url, 201, t1, t2);
    let disks_url = "/v1/disks?project=test-project";
    verify_entry(&items[2], "disk_create", disks_url, 201, t1, t2);

    // Verify delete entries
    verify_entry(&items[3], "instance_delete", instance_del_url, 204, t2, t3);
    verify_entry(&items[4], "disk_delete", disk_del_url, 204, t2, t3);
    verify_entry(&items[5], "project_delete", project_del_url, 204, t2, t3);
}

fn verify_entry(
    entry: &views::AuditLogEntry,
    operation_id: &str,
    request_uri: &str,
    http_status_code: u16,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
) {
    // Verify operation-specific fields
    assert_eq!(entry.operation_id, operation_id);
    assert_eq!(entry.request_uri, request_uri);
    assert_eq!(
        entry.result,
        views::AuditLogEntryResult::Success { http_status_code }
    );
    assert!(entry.time_started >= start_time && entry.time_started <= end_time);

    // Verify fields common to all test-generated entries
    assert_eq!(
        entry.actor,
        views::AuditLogEntryActor::SiloUser {
            silo_user_id: USER_TEST_PRIVILEGED.id(),
            silo_id: DEFAULT_SILO_ID,
        }
    );
    assert_eq!(entry.source_ip.to_string(), "127.0.0.1");
    assert_eq!(entry.auth_method, Some("spoof".to_string()));
    assert!(entry.time_completed > entry.time_started);
}
