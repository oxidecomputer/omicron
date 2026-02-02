// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::{DateTime, Utc};
use dropshot::{ResultsPage, test_util::ClientTestContext};
use http::{Method, StatusCode, header};
use nexus_db_queries::authn::USER_TEST_PRIVILEGED;
use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use nexus_test_utils::resource_helpers::{
    DiskTest, create_console_session, create_default_ip_pools, create_disk,
    create_instance_with, create_local_user, create_project, create_silo,
    get_device_token, grant_iam, object_create_error, object_create_no_body,
    object_delete, objects_list_page_authz, test_params,
};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::{params, shared, views};
use nexus_types::{identity::Asset, silo::DEFAULT_SILO_ID};
use omicron_common::api::external::{
    IdentityMetadataCreateParams, InstanceAutoRestartPolicy,
    InstanceCpuPlatform, Name, UserId,
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
    // Use a large limit to avoid pagination hiding results
    let mut qs =
        vec![format!("start_time={}", to_q(start)), "limit=1000".to_string()];
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
    let session_token = create_console_session(ctx).await;
    let session_cookie = format!("session={}", &session_token);

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
    let long_user_agent = "A".repeat(300);
    let long_query_value = "B".repeat(600);
    let long_uri =
        format!("/v1/projects?very_long_parameter={}", long_query_value);
    RequestBuilder::new(client, Method::POST, &long_uri)
        .body(Some(&body))
        .header(header::COOKIE, session_cookie.clone())
        .header("User-Agent", &long_user_agent)
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
    assert_eq!(e1.auth_method, Some(views::AuthMethod::Spoof));
    assert_eq!(e1.credential_id, None); // spoof auth has no credential
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
    assert_eq!(e2.credential_id, None); // unauthenticated
    assert!(e2.time_started >= t2 && e2.time_started <= t3);
    assert!(e2.time_completed > e2.time_started);

    // login attempts are unauthenticated (until the user is authenticated)
    assert_eq!(e2.actor, views::AuditLogEntryActor::Unauthenticated);

    // session create was the test suite user in the test suite silo, which
    // is different from the privileged user, so we need to fetch the user
    // and silo ID using the session to check them against the audit log
    let session_authn = AuthnMode::Session(session_token);
    let me: views::CurrentUser = NexusRequest::object_get(client, "/v1/me")
        .authn_as(session_authn.clone())
        .execute_and_parse_unwrap()
        .await;

    // get the session ID to verify credential_id
    let sessions_url = format!("/v1/users/{}/sessions", me.user.id);
    let sessions: ResultsPage<views::ConsoleSession> =
        NexusRequest::object_get(client, &sessions_url)
            .authn_as(session_authn)
            .execute_and_parse_unwrap()
            .await;
    assert_eq!(sessions.items.len(), 1);
    let session_id = sessions.items[0].id;

    // third one was done with the session cookie, reflected in auth_method
    assert_eq!(e3.request_uri.len(), 512);
    assert!(
        e3.request_uri.starts_with("/v1/projects?very_long_parameter=BBBBB")
    );
    assert_eq!(e3.operation_id, "project_create");
    assert_eq!(e3.source_ip.to_string(), "127.0.0.1");
    assert_eq!(e3.user_agent.clone().unwrap(), "A".repeat(256));
    assert_eq!(e3.auth_method, Some(views::AuthMethod::SessionCookie));
    assert_eq!(e3.credential_id, Some(session_id));
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
        log.next_page.clone().unwrap()
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

    // Set up disk test infrastructure (this may create audit log entries
    // for covered endpoints, but we're testing the explicit CRUD ops below)
    DiskTest::new(&ctx).await;
    create_default_ip_pools(client).await;

    // Start timing AFTER setup so we only count entries from our test ops
    let t1 = Utc::now();

    let _project = create_project(client, "test-project").await;
    let _instance = create_instance_with(
        client,
        "test-project",
        "test-instance",
        &params::InstanceNetworkInterfaceAttachment::DefaultIpv4,
        Vec::<params::InstanceDiskAttachment>::new(),
        Vec::<params::ExternalIpCreate>::new(),
        false, // start=false, so instance is created in stopped state
        None::<InstanceAutoRestartPolicy>,
        None::<InstanceCpuPlatform>,
        Vec::new(),
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

    // Fetch audit log entries created during our test ops (after t1)
    let audit_log = fetch_log(client, t1, None).await;
    assert_eq!(audit_log.items.len(), 8);

    let items = &audit_log.items;

    // Verify create entries
    verify_entry(&items[0], "project_create", "/v1/projects", 201, t1, t2);
    let instances_url = "/v1/instances?project=test-project";
    verify_entry(&items[1], "instance_create", instances_url, 201, t1, t2);
    let disks_url = "/v1/disks?project=test-project";
    verify_entry(&items[2], "disk_create", disks_url, 201, t1, t2);

    // Verify delete entries (instance, disk, subnet, vpc, project)
    verify_entry(&items[3], "instance_delete", instance_del_url, 204, t2, t3);
    verify_entry(&items[4], "disk_delete", disk_del_url, 204, t2, t3);
    verify_entry(
        &items[5],
        "vpc_subnet_delete",
        subnet_delete_url,
        204,
        t2,
        t3,
    );
    verify_entry(&items[6], "vpc_delete", vpc_delete_url, 204, t2, t3);
    verify_entry(&items[7], "project_delete", project_del_url, 204, t2, t3);
}

/// Test that mutating endpoints in VERIFY_ENDPOINTS create audit log entries.
/// This is a coverage test to catch endpoints that forget to add audit logging.
/// The snapshot file lists endpoints that are known to not have audit logging.
/// As audit logging is added to endpoints, they should be removed from the file.
#[nexus_test]
async fn test_audit_log_coverage(ctx: &ControlPlaneTestContext) {
    use super::endpoint_coverage::ApiOperations;
    use super::endpoints::{AllowedMethod, VERIFY_ENDPOINTS};
    use nexus_test_utils::http_testing::{AuthnMode, NexusRequest};
    use std::collections::BTreeMap;

    let client = &ctx.external_client;

    let api_operations = ApiOperations::new();

    // Track mutating endpoints we haven't tested yet (not in VERIFY_ENDPOINTS).
    let mut untested_mutating: BTreeMap<String, (String, String)> =
        api_operations
            .iter()
            .filter(|op| {
                matches!(
                    op.method.as_str(),
                    "POST" | "PUT" | "PATCH" | "DELETE"
                )
            })
            .map(|op| {
                (
                    op.operation_id.clone(),
                    (op.method.to_lowercase(), op.path.clone()),
                )
            })
            .collect();

    // Set up resources needed by many endpoints
    DiskTest::new(&ctx).await;
    create_default_ip_pools(client).await;
    let _project = create_project(client, "demo-project").await;

    let t_start = Utc::now();

    let mut missing_audit: BTreeMap<String, (String, String)> = BTreeMap::new();
    let mut unexpected_get_audit: BTreeMap<String, (String, String)> =
        BTreeMap::new();

    for endpoint in &*VERIFY_ENDPOINTS {
        for method in &endpoint.allowed_methods {
            let is_mutating = match method {
                AllowedMethod::Post(_)
                | AllowedMethod::Put(_)
                | AllowedMethod::Delete => true,
                AllowedMethod::Get
                | AllowedMethod::GetNonexistent
                | AllowedMethod::GetUnimplemented
                | AllowedMethod::GetVolatile
                | AllowedMethod::GetWebsocket
                | AllowedMethod::Head
                | AllowedMethod::HeadNonexistent => false,
            };

            let before = fetch_log(client, t_start, None).await.items.len();

            // Make authenticated request as unprivileged user. This will fail
            // authz but should still create an audit log entry if the endpoint
            // has audit logging. Using unprivileged avoids actually modifying
            // resources (e.g., removing our own permissions via fleet policy).
            let http_method = method.http_method().clone();
            let body = method.body().cloned();

            // Replace {id} placeholders with a valid UUID so path parsing
            // succeeds and the request reaches the handler. The actual UUID
            // doesn't matter since we're testing as unprivileged and will fail
            // authz anyway - we just need the request to reach the handler.
            let url = endpoint
                .url
                .replace("{id}", "00000000-0000-0000-0000-000000000000");

            let result = NexusRequest::new(
                RequestBuilder::new(client, http_method.clone(), &url)
                    .body(body.as_ref())
                    .expect_status(None), // accept any status
            )
            .authn_as(AuthnMode::UnprivilegedUser)
            .execute()
            .await;

            if result.is_err() {
                // Request itself failed (connection error, etc), skip
                continue;
            }

            let after = fetch_log(client, t_start, None).await.items.len();

            // Find the operation info from the API description
            let method_str = http_method.to_string();

            let (op_id, path_template) = api_operations
                .find(&method_str, endpoint.url)
                .map(|op| (op.operation_id.clone(), op.path.clone()))
                .unwrap_or_else(|| {
                    let url_path = endpoint.url.split('?').next().unwrap();
                    (String::from("unknown"), url_path.to_string())
                });

            // Mark this endpoint as tested
            untested_mutating.remove(&op_id);

            if is_mutating {
                // Mutating endpoints SHOULD have audit logging
                if after <= before {
                    missing_audit.insert(
                        op_id,
                        (method_str.to_lowercase(), path_template),
                    );
                }
            } else {
                // GET endpoints should NOT have audit logging
                if after > before {
                    unexpected_get_audit.insert(
                        op_id,
                        (method_str.to_lowercase(), path_template),
                    );
                }
            }
        }
    }

    let mut output =
        String::from("Mutating endpoints without audit logging:\n");
    for (op_id, (method, path)) in &missing_audit {
        output.push_str(&format!("{:44} ({:6} {:?})\n", op_id, method, path));
    }

    output.push_str(
        "\nMutating endpoints not tested (not in VERIFY_ENDPOINTS):\n",
    );
    for (op_id, (method, path)) in &untested_mutating {
        output.push_str(&format!("{:44} ({:6} {:?})\n", op_id, method, path));
    }

    // Print a helpful message when there are new uncovered endpoints
    let expected_path = "tests/output/uncovered-audit-log-endpoints.txt";
    let expected = std::fs::read_to_string(expected_path).unwrap_or_default();
    let expected_ops: std::collections::HashSet<&str> = expected
        .lines()
        .skip(1) // skip the header line
        .filter_map(|line| line.split_whitespace().next())
        .collect();
    let unexpected_uncovered: Vec<_> = missing_audit
        .keys()
        .filter(|op| !expected_ops.contains(op.as_str()))
        .collect();
    if !unexpected_uncovered.is_empty() {
        eprintln!();
        eprintln!(
            "======================================================================="
        );
        eprintln!("ENDPOINTS MISSING AUDIT LOGGING:");
        for op in &unexpected_uncovered {
            eprintln!("  - {}", op);
        }
        eprintln!();
        eprintln!(
            "To add audit logging, wrap your handler function in `audit_and_time`."
        );
        eprintln!(
            "See http_entrypoints.rs for examples and context.rs for documentation."
        );
        eprintln!();
        eprintln!(
            "If the endpoint is read-only despite using POST (like the timeseries"
        );
        eprintln!(
            "query endpoints), add it to uncovered-audit-log-endpoints.txt."
        );
        eprintln!(
            "======================================================================="
        );
        eprintln!();
    }

    // NOTE: We intentionally do NOT use expectorate's assert_contents here
    // because we don't want EXPECTORATE=overwrite to allow people to
    // accidentally add uncovered endpoints to the allowlist.
    similar_asserts::assert_eq!(
        expected,
        output,
        "left: uncovered-audit-log-endpoints.txt, right: actual"
    );

    // Check for GET endpoints that unexpectedly have audit logging
    let mut get_output = String::from("GET endpoints with audit logging:\n");
    for (op_id, (method, path)) in &unexpected_get_audit {
        get_output
            .push_str(&format!("{:44} ({:6} {:?})\n", op_id, method, path));
    }

    let get_expected_path = "tests/output/audited-get-endpoints.txt";
    let get_expected =
        std::fs::read_to_string(get_expected_path).unwrap_or_default();
    let get_expected_ops: std::collections::HashSet<&str> = get_expected
        .lines()
        .skip(1) // skip the header line
        .filter_map(|line| line.split_whitespace().next())
        .collect();
    let unexpected_audited: Vec<_> = unexpected_get_audit
        .keys()
        .filter(|op| !get_expected_ops.contains(op.as_str()))
        .collect();
    if !unexpected_audited.is_empty() {
        eprintln!();
        eprintln!(
            "======================================================================="
        );
        eprintln!("GET ENDPOINTS WITH UNEXPECTED AUDIT LOGGING:");
        for op in &unexpected_audited {
            eprintln!("  - {}", op);
        }
        eprintln!();
        eprintln!(
            "GET endpoints should not have audit logging because they don't"
        );
        eprintln!(
            "modify state. If this endpoint was intentionally audited (rare),"
        );
        eprintln!("add it to audited-get-endpoints.txt.");
        eprintln!(
            "======================================================================="
        );
        eprintln!();
    }

    // NOTE: We intentionally do NOT use expectorate's assert_contents here
    // because we don't want EXPECTORATE=overwrite to allow people to
    // accidentally add audited GET endpoints to the list.
    similar_asserts::assert_eq!(
        get_expected,
        get_output,
        "left: audited-get-endpoints.txt, right: actual"
    );
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
    assert_eq!(entry.auth_method, Some(views::AuthMethod::Spoof));
    assert!(entry.time_completed > entry.time_started);
}

/// Test that AccessToken auth method is correctly recorded in the audit log
#[nexus_test]
async fn test_audit_log_access_token_auth(ctx: &ControlPlaneTestContext) {
    let client = &ctx.external_client;

    let token_grant = get_device_token(client, AuthnMode::PrivilegedUser).await;

    let t1 = Utc::now();

    // Make an audited request using the access token
    let body = &params::ProjectCreate {
        identity: IdentityMetadataCreateParams {
            name: "token-project".parse().unwrap(),
            description: "created with access token".to_string(),
        },
    };
    RequestBuilder::new(client, Method::POST, "/v1/projects")
        .body(Some(&body))
        .header(
            header::AUTHORIZATION,
            format!("Bearer {}", token_grant.access_token),
        )
        .expect_status(Some(StatusCode::CREATED))
        .execute()
        .await
        .expect("failed to create project with access token");

    let t2 = Utc::now();

    // Fetch the audit log and find the entry
    let audit_log = fetch_log(client, t1, Some(t2)).await;
    assert_eq!(audit_log.items.len(), 1);

    let entry = &audit_log.items[0];
    assert_eq!(entry.operation_id, "project_create");
    assert_eq!(entry.request_uri, "/v1/projects");
    assert_eq!(entry.auth_method, Some(views::AuthMethod::AccessToken));
    assert_eq!(entry.credential_id, Some(token_grant.token_id));
    assert_eq!(
        entry.actor,
        views::AuditLogEntryActor::SiloUser {
            silo_user_id: USER_TEST_PRIVILEGED.id(),
            silo_id: DEFAULT_SILO_ID,
        }
    );
    assert_eq!(
        entry.result,
        views::AuditLogEntryResult::Success { http_status_code: 201 }
    );
}

/// Test that ScimToken auth method is correctly recorded in the audit log
#[nexus_test]
async fn test_audit_log_scim_token_auth(ctx: &ControlPlaneTestContext) {
    let client = &ctx.external_client;

    // Create a SAML+SCIM silo (required for SCIM tokens)
    const SILO_NAME: &str = "scim-audit-test-silo";
    let silo = create_silo(
        client,
        SILO_NAME,
        true,
        shared::SiloIdentityMode::SamlScim,
    )
    .await;

    // Grant the privileged user admin role on this silo so they can create tokens
    grant_iam(
        client,
        &format!("/v1/system/silos/{SILO_NAME}"),
        shared::SiloRole::Admin,
        USER_TEST_PRIVILEGED.id(),
        AuthnMode::PrivilegedUser,
    )
    .await;

    // Create a SCIM token
    let url = format!("/v1/system/scim/tokens?silo={SILO_NAME}");
    let created_token: views::ScimClientBearerTokenValue =
        object_create_no_body(client, &url).await;

    let t1 = Utc::now();

    // Make an audited SCIM request using the token
    RequestBuilder::new(client, Method::GET, "/scim/v2/Users")
        .header(
            header::AUTHORIZATION,
            format!("Bearer {}", created_token.bearer_token),
        )
        .allow_non_dropshot_errors()
        .expect_status(Some(StatusCode::OK))
        .execute()
        .await
        .expect("failed to list SCIM users");

    let t2 = Utc::now();

    // Fetch the audit log and find the entry
    let audit_log = fetch_log(client, t1, Some(t2)).await;
    assert_eq!(audit_log.items.len(), 1);

    let entry = &audit_log.items[0];
    assert_eq!(entry.operation_id, "scim_v2_list_users");
    assert_eq!(entry.request_uri, "/scim/v2/Users");
    assert_eq!(entry.auth_method, Some(views::AuthMethod::ScimToken));
    assert_eq!(entry.credential_id, Some(created_token.id));
    assert_eq!(
        entry.actor,
        views::AuditLogEntryActor::Scim { silo_id: silo.identity.id }
    );
    assert_eq!(
        entry.result,
        views::AuditLogEntryResult::Success { http_status_code: 200 }
    );
}
