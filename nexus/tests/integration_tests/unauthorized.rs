// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Verify the behavior of API endpoints when hit by unauthenticated and
//! unauthorized users

use super::endpoints::*;
use crate::integration_tests::saml::SAML_IDP_DESCRIPTOR;
use crate::integration_tests::updates::TestTrustRoot;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use dropshot::HttpErrorResponseBody;
use dropshot::test_util::ClientTestContext;
use headers::authorization::Credentials;
use http::StatusCode;
use http::method::Method;
use httptest::{Expectation, ServerBuilder, matchers::*, responders::*};
use nexus_db_queries::authn::external::spoof;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::http_testing::TestResponse;
use nexus_test_utils::resource_helpers::TestDataset;
use nexus_test_utils::test_setup;
use omicron_common::disk::DatasetKind;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::ZpoolUuid;
use std::sync::LazyLock;

type DiskTest<'a> =
    nexus_test_utils::resource_helpers::DiskTest<'a, omicron_nexus::Server>;

// This test hits a list Nexus API endpoints using both unauthenticated and
// unauthorized requests to make sure we get the expected behavior (generally:
// 401, 403, or 404).  This is trickier than it sounds because the appropriate
// error code depends on what the user was trying to do and what other
// permissions they have on the resource.  Notably, if you try to do anything
// with a resource that you're not even supposed to be able to see, you should
// get a 404 "Not Found", not a 403 "Forbidden".  It's critical to get this
// right because the alternative can leak information to a potential attacker.
//
// Fortunately, most endpoints behave the same way when it comes to
// unauthenticated or unauthorized requests so it's possible to exhaustively
// test much of the API.
//
// This test works in two phases.  First, we execute a sequence of setup
// requests that create all the resources that we're going to test with.  Then
// we run through the list of endpoints we're going to test and verify each one.
// See `verify_endpoint()` for exactly what we do for each one.
//
// TODO-coverage:
// * It would be good to add a built-in test user that can read everything in
//   the world and use that to exercise 404 vs. 401/403 behavior.
// * When we finish authz, maybe the hardcoded information here can come instead
//   from the OpenAPI spec?
// * For each endpoint that hits a real resource, we should hit the same
//   endpoint with a non-existent resource to ensure that we get the same result
//   (so that we don't leak information about existence based on, say, 401 vs.
//   403).
//
// Uploading a TUF repository requires a multithreaded runtime with 2 worker
// threads.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_unauthorized() {
    let cptestctx =
        test_setup::<omicron_nexus::Server>("test_unauthorized", 0).await;

    let mut disk_test = DiskTest::new(&cptestctx).await;
    let sled_id = cptestctx.first_sled_id();
    disk_test
        .add_zpool_with_datasets_ext(
            sled_id,
            nexus_test_utils::PHYSICAL_DISK_UUID.parse().unwrap(),
            ZpoolUuid::new_v4(),
            vec![
                TestDataset {
                    id: DatasetUuid::new_v4(),
                    kind: DatasetKind::Crucible,
                },
                TestDataset {
                    id: DatasetUuid::new_v4(),
                    kind: DatasetKind::Debug,
                },
            ],
            DiskTest::DEFAULT_ZPOOL_SIZE_GIB,
        )
        .await;
    disk_test.propagate_datasets_to_sleds().await;

    let client = &cptestctx.external_client;
    let log = &cptestctx.logctx.log;
    let mut setup_results = std::collections::BTreeMap::new();

    // Create test data.
    info!(log, "setting up resource hierarchy");
    for request in &*SETUP_REQUESTS {
        let (url, result, id_routes) = match request {
            SetupReq::Get { url, id_routes } => (
                url,
                NexusRequest::object_get(client, url)
                    .authn_as(AuthnMode::PrivilegedUser)
                    .execute()
                    .await
                    .map_err(|e| panic!("Failed to GET from URL: {url}, {e}"))
                    .unwrap(),
                id_routes,
            ),
            SetupReq::Post { url, body, id_routes } => (
                url,
                NexusRequest::objects_post(client, url, body)
                    .authn_as(AuthnMode::PrivilegedUser)
                    .execute()
                    .await
                    .map_err(|e| panic!("Failed to POST to URL: {url}, {e}"))
                    .unwrap(),
                id_routes,
            ),
        };

        setup_results.insert(url, result.clone());
        id_routes.iter().for_each(|id_route| {
            setup_results.insert(id_route, result.clone());
        });
    }

    // Special test data: upload a fake repository with the system release
    // "1.0.0". The resources do not need to be added to `setup_results` as we
    // have already added a separate trust root to verify that endpoint, and
    // repositories are fetched by system release.
    let trust_root = TestTrustRoot::generate().await.unwrap();
    trust_root
        .to_upload_request(client, StatusCode::CREATED)
        .execute()
        .await
        .unwrap();
    trust_root
        .assemble_repo(&log, &[])
        .await
        .unwrap()
        .into_upload_request(client, StatusCode::OK)
        .execute()
        .await
        .unwrap();

    // Insert a SCIM client bearer token with a known UUID - normally these are
    // completely random.

    {
        use nexus_db_model::ScimClientBearerToken;
        use nexus_types::silo::DEFAULT_SILO_ID;

        let now = Utc::now();

        let new_token = ScimClientBearerToken {
            id: "7885144e-9c75-47f7-a97d-7dfc58e1186c".parse().unwrap(),
            time_created: now,
            time_deleted: None,
            time_expires: Some(now),
            silo_id: DEFAULT_SILO_ID,
            bearer_token: String::from("testpost"),
        };

        let nexus = &cptestctx.server.server_context().nexus;
        let conn = nexus.datastore().pool_connection_for_tests().await.unwrap();

        use nexus_db_schema::schema::scim_client_bearer_token::dsl;
        diesel::insert_into(dsl::scim_client_bearer_token)
            .values(new_token.clone())
            .execute_async(&*conn)
            .await
            .unwrap();
    }

    // Verify the hardcoded endpoints.
    info!(log, "verifying endpoints");
    print!("{}", VERIFY_HEADER);
    for endpoint in &*VERIFY_ENDPOINTS {
        let setup_response = setup_results.get(&endpoint.url);
        verify_endpoint(&log, client, endpoint, setup_response).await;
    }

    cptestctx.teardown().await;
}

const VERIFY_HEADER: &str = r#"
SUMMARY OF REQUESTS MADE

KEY, USING HEADER AND EXAMPLE ROW:

          +----------------------------> privileged GET (expects 200 or 500)
          |                              (digit = last digit of status code)
          |
          |                          +-> privileged GET (expects same as above)
          |                          |   (digit = last digit of status code)
          |                          |   ('-' => skipped (N/A))
          ^                          ^
HEADER:   G GET  PUT  POST DEL  TRCE G  URL
EXAMPLE:  0 3111 5555 3111 5555 5555 0  /organizations
    ROW     ^^^^
            ||||                      TEST CASES FOR EACH HTTP METHOD:
            +|||----------------------< authenticated, unauthorized request
             +||----------------------< unauthenticated request
              +|----------------------< bad authentication: no such user
               +----------------------< bad authentication: invalid syntax

            \__/ \__/ \__/ \__/ \__/
            GET  PUT  etc.  The test cases are repeated for each HTTP method.

            The number in each cell is the last digit of the 400-level response
            that was expected for this test case.

    In this case, an unauthenticated request to "GET /organizations" returned
    401.  All requests to "PUT /organizations" returned 405.

G GET  PUT  POST DEL  TRCE G  URL
"#;

//
// SETUP PHASE
//

/// Describes a request made during the setup phase to create a resource that
/// we'll use later in the verification phase
///
/// The setup phase takes a list of `SetupReq` enums and issues a `GET` or
/// `POST` request to each one's `url`. `id_results` is a list of URLs that are
/// associated to the results of the setup request with any `{id}` params in the
/// URL replaced with the result's URL. This is used to later verify ID
/// endpoints without first having to know the ID.
enum SetupReq {
    Get {
        url: &'static str,
        id_routes: Vec<&'static str>,
    },
    Post {
        url: &'static str,
        body: serde_json::Value,
        id_routes: Vec<&'static str>,
    },
}

pub static HTTP_SERVER: LazyLock<httptest::Server> =
    LazyLock::new(|| {
        // Run a httptest server
        let server = ServerBuilder::new().run().unwrap();

        // Fake some data
        server.expect(
            Expectation::matching(request::method_path("HEAD", "/image.raw"))
                .times(1..)
                .respond_with(status_code(200).append_header(
                    "Content-Length",
                    format!("{}", 4096 * 1000),
                )),
        );

        server.expect(
            Expectation::matching(request::method_path("GET", "/descriptor"))
                .times(1..)
                .respond_with(status_code(200).body(SAML_IDP_DESCRIPTOR)),
        );

        server
    });

/// List of requests to execute at setup time
static SETUP_REQUESTS: LazyLock<Vec<SetupReq>> = LazyLock::new(|| {
    vec![
        // Create a separate Silo
        SetupReq::Post {
            url: "/v1/system/silos",
            body: serde_json::to_value(&*DEMO_SILO_CREATE).unwrap(),
            id_routes: vec![],
        },
        // Create a local User
        SetupReq::Post {
            url: &DEMO_SILO_USERS_CREATE_URL,
            body: serde_json::to_value(&*DEMO_USER_CREATE).unwrap(),
            id_routes: vec![
                &*DEMO_SILO_USER_ID_GET_URL,
                &*DEMO_SILO_USER_ID_DELETE_URL,
                &*DEMO_SILO_USER_ID_SET_PASSWORD_URL,
                &*DEMO_SILO_USER_ID_IN_SILO_URL,
                &*DEMO_SILO_USER_TOKEN_LIST_URL,
                &*DEMO_SILO_USER_SESSION_LIST_URL,
                &*DEMO_SILO_USER_LOGOUT_URL,
            ],
        },
        // Create the default IP pool
        SetupReq::Post {
            url: &DEMO_SYSTEM_IP_POOLS_URL,
            body: serde_json::to_value(&*DEMO_IP_POOL_CREATE).unwrap(),
            id_routes: vec!["/v1/system/ip-pools/{id}"],
        },
        // Create an IP pool range
        SetupReq::Post {
            url: &DEMO_IP_POOL_RANGES_ADD_URL,
            body: serde_json::to_value(&*DEMO_IP_POOL_RANGE).unwrap(),
            id_routes: vec![],
        },
        // Link default pool to default silo
        SetupReq::Post {
            url: &DEMO_IP_POOL_SILOS_URL,
            body: serde_json::to_value(&*DEMO_IP_POOL_SILOS_BODY).unwrap(),
            id_routes: vec![],
        },
        // Create a Project in the Organization
        SetupReq::Post {
            url: "/v1/projects",
            body: serde_json::to_value(&*DEMO_PROJECT_CREATE).unwrap(),
            id_routes: vec![],
        },
        // Create a VPC in the Project
        SetupReq::Post {
            url: &DEMO_PROJECT_URL_VPCS,
            body: serde_json::to_value(&*DEMO_VPC_CREATE).unwrap(),
            id_routes: vec!["/by-id/vpcs/{id}"],
        },
        // Create a VPC Subnet in the Vpc
        SetupReq::Post {
            url: &DEMO_VPC_URL_SUBNETS,
            body: serde_json::to_value(&*DEMO_VPC_SUBNET_CREATE).unwrap(),
            id_routes: vec!["/by-id/vpc-subnets/{id}"],
        },
        // Create a VPC Router in the Vpc
        SetupReq::Post {
            url: &DEMO_VPC_URL_ROUTERS,
            body: serde_json::to_value(&*DEMO_VPC_ROUTER_CREATE).unwrap(),
            id_routes: vec!["/by-id/vpc-routers/{id}"],
        },
        // Create a VPC Router in the Vpc
        SetupReq::Post {
            url: &DEMO_VPC_ROUTER_URL_ROUTES,
            body: serde_json::to_value(&*DEMO_ROUTER_ROUTE_CREATE).unwrap(),
            id_routes: vec!["/by-id/vpc-router-routes/{id}"],
        },
        // Create a Disk in the Project
        SetupReq::Post {
            url: &DEMO_DISKS_URL,
            body: serde_json::to_value(&*DEMO_DISK_CREATE).unwrap(),
            id_routes: vec!["/v1/disks/{id}"],
        },
        // Create a Disk in the Project with state ImportReady
        SetupReq::Post {
            url: &DEMO_DISKS_URL,
            body: serde_json::to_value(&*DEMO_IMPORT_DISK_CREATE).unwrap(),
            id_routes: vec!["/v1/disks/{id}"],
        },
        // Create an Instance in the Project
        SetupReq::Post {
            url: &DEMO_PROJECT_URL_INSTANCES,
            body: serde_json::to_value(&*DEMO_INSTANCE_CREATE).unwrap(),
            id_routes: vec!["/v1/instances/{id}"],
        },
        // Create a stopped Instance in the Project
        SetupReq::Post {
            url: &DEMO_PROJECT_URL_INSTANCES,
            body: serde_json::to_value(&*DEMO_STOPPED_INSTANCE_CREATE).unwrap(),
            id_routes: vec!["/v1/instances/{id}"],
        },
        // Create a multicast IP pool
        SetupReq::Post {
            url: &DEMO_SYSTEM_IP_POOLS_URL,
            body: serde_json::to_value(&*DEMO_MULTICAST_IP_POOL_CREATE)
                .unwrap(),
            id_routes: vec!["/v1/system/ip-pools/{id}"],
        },
        // Create a multicast IP pool range
        SetupReq::Post {
            url: &DEMO_MULTICAST_IP_POOL_RANGES_ADD_URL,
            body: serde_json::to_value(&*DEMO_MULTICAST_IP_POOL_RANGE).unwrap(),
            id_routes: vec![],
        },
        // Link multicast pool to default silo
        SetupReq::Post {
            url: &DEMO_MULTICAST_IP_POOL_SILOS_URL,
            body: serde_json::to_value(&*DEMO_MULTICAST_IP_POOL_SILOS_BODY)
                .unwrap(),
            id_routes: vec![],
        },
        // Create a multicast group in the Project
        SetupReq::Post {
            url: &MULTICAST_GROUPS_URL,
            body: serde_json::to_value(&*DEMO_MULTICAST_GROUP_CREATE).unwrap(),
            id_routes: vec!["/v1/multicast-groups/{id}"],
        },
        // Create an affinity group in the Project
        SetupReq::Post {
            url: &DEMO_PROJECT_URL_AFFINITY_GROUPS,
            body: serde_json::to_value(&*DEMO_AFFINITY_GROUP_CREATE).unwrap(),
            id_routes: vec!["/v1/affinity-groups/{id}"],
        },
        // Add an instance to the affinity group
        SetupReq::Post {
            url: &DEMO_AFFINITY_GROUP_INSTANCE_MEMBER_URL,
            body: serde_json::Value::Null,
            id_routes: vec![],
        },
        // Create an anti-affinity group in the Project
        SetupReq::Post {
            url: &DEMO_PROJECT_URL_ANTI_AFFINITY_GROUPS,
            body: serde_json::to_value(&*DEMO_ANTI_AFFINITY_GROUP_CREATE)
                .unwrap(),
            id_routes: vec!["/v1/anti-affinity-groups/{id}"],
        },
        // Add an instance to the anti-affinity group
        SetupReq::Post {
            url: &DEMO_ANTI_AFFINITY_GROUP_INSTANCE_MEMBER_URL,
            body: serde_json::Value::Null,
            id_routes: vec![],
        },
        // Lookup the previously created NIC
        SetupReq::Get {
            url: &DEMO_INSTANCE_NIC_URL,
            id_routes: vec!["/by-id/network-interfaces/{id}"],
        },
        // Create a Snapshot in the Project
        SetupReq::Post {
            url: &DEMO_PROJECT_URL_SNAPSHOTS,
            body: serde_json::to_value(&*DEMO_SNAPSHOT_CREATE).unwrap(),
            id_routes: vec!["/by-id/snapshots/{id}"],
        },
        // Create an Image in the Project
        SetupReq::Post {
            url: &DEMO_PROJECT_IMAGES_URL,
            body: serde_json::to_value(&*DEMO_IMAGE_CREATE).unwrap(),
            id_routes: vec!["/v1/images/{id}"],
        },
        // Create a Floating IP in the project
        SetupReq::Post {
            url: &DEMO_PROJECT_URL_FIPS,
            body: serde_json::to_value(&*DEMO_FLOAT_IP_CREATE).unwrap(),
            id_routes: vec!["/v1/floating-ips/{id}"],
        },
        // Create a SAML identity provider
        SetupReq::Post {
            url: &SAML_IDENTITY_PROVIDERS_URL,
            body: serde_json::to_value(&*SAML_IDENTITY_PROVIDER).unwrap(),
            id_routes: vec![],
        },
        // Create a SSH key
        SetupReq::Post {
            url: &DEMO_SSHKEYS_URL,
            body: serde_json::to_value(&*DEMO_SSHKEY_CREATE).unwrap(),
            id_routes: vec![],
        },
        // Create a Certificate
        SetupReq::Post {
            url: &DEMO_CERTIFICATES_URL,
            body: serde_json::to_value(&*DEMO_CERTIFICATE_CREATE).unwrap(),
            id_routes: vec![],
        },
        // Create a Support Bundle
        SetupReq::Post {
            url: &SUPPORT_BUNDLES_URL,
            body: serde_json::to_value(
                &nexus_types::external_api::params::SupportBundleCreate {
                    user_comment: None,
                },
            )
            .unwrap(),
            id_routes: vec!["/experimental/v1/system/support-bundles/{id}"],
        },
        // Create a trusted root for updates
        SetupReq::Post {
            url: &DEMO_UPDATE_TRUST_ROOTS_URL,
            body: DEMO_UPDATE_TRUST_ROOT_CREATE.clone(),
            id_routes: vec![&*DEMO_UPDATE_TRUST_ROOT_URL],
        },
        // Create a webhook receiver
        SetupReq::Post {
            url: &WEBHOOK_RECEIVERS_URL,
            body: serde_json::to_value(&*DEMO_WEBHOOK_RECEIVER_CREATE).unwrap(),
            id_routes: vec![],
        },
        // Create a secret for that receiver
        SetupReq::Post {
            url: &DEMO_WEBHOOK_SECRETS_URL,
            body: serde_json::to_value(&*DEMO_WEBHOOK_SECRET_CREATE).unwrap(),
            id_routes: vec![&*DEMO_WEBHOOK_SECRET_DELETE_URL],
        },
    ]
});

/// Contents returned from an endpoint that creates a resource that has an id
///
/// This is a subset of `IdentityMetadata`.  `IdentityMetadata` includes other
/// fields (like "name") that are not present on all objects.
#[derive(serde::Deserialize)]
struct IdMetadata {
    id: String,
}

/// Verifies a single API endpoint, described with `endpoint`
///
/// (Technically, a single `VerifyEndpoint` struct describes an HTTP resource,
/// like "/v1/organizations".  There are several API endpoints there, like "GET
/// /organizations" and "POST /organizations".  We're a little loose with the
/// terminology here.)
///
/// This test makes requests using a bunch of different HTTP methods: GET, PUT,
/// POST, and DELETE because the API makes heavy use of those; plus TRACE as a
/// sort of control to make sure we get back 405 "Method Not Allowed" for some
/// other method.  (This is not really related to authorization per se, but
/// getting 405 back for TRACE lets us know that the server correctly handles
/// unsupported methods, which _is_ a security issue.)
///
/// Endpoints usually only support a few of these methods.
/// `endpoint.allowed_methods` tells us which ones and provides request bodies
/// to use for PUT and POST requests.  We always make requests for all of these
/// HTTP methods, even the unsupported ones.  We expect to get back a 405 for
/// the unsupported ones.  (This helps verify that we don't accidentally support
/// DELETE on a resource, for example!)
///
/// The expected result for each resource is a little tricky:
/// - If the requested method is not allowed, we always expect 405 "Method Not
///   Allowed".
/// - If the resource is not publicly visible, then we expect a 404 for both
///   unauthenticated and unauthorized users.
/// - If the resource is publicly visible (based on `endpoint.visibility`), then
///   we expect a 401 for unauthenticated users and a 403 for unauthenticated,
///   unauthorized users.  Note that "visible" here doesn't mean "accessible".
///   We assume that everybody is allowed to know that "/v1/organizations" exists.
///   But they're not necessarily allowed to _use_ it.  That's why it's correct
///   to get 401/403 on "GET /organizations", even though it's a GET and you
///   might think all GETs to things you can't access should be 404s.
///
/// We also make requests to each resource with bogus credentials of various
/// forms to make sure they're all correctingly using the authentication
/// subsystem.
///
/// We also make one request to GET the endpoint using a privileged user to
/// ensure that we get a 200.  (If that returned 404, then there's probably some
/// other bug causing the endpoint to return a 404, and it would be wrong for us
/// to believe we correctly got a 404 for an unauthorized user because they were
/// unauthorized.)
///
/// There are some weird cases here.  For example, if you try to "POST
/// /organizations/demo-org", then you'll get back a 405, even if you can't see
/// "demo-org" (which you would normally think would result in a 404).  This is
/// a little weird in that you can "learn" about what API endpoints exist.  But
/// you already know that because we publish the API spec.  And you can't learn
/// what _resources_ actually exist this way.
async fn verify_endpoint(
    log: &slog::Logger,
    client: &ClientTestContext,
    endpoint: &VerifyEndpoint,
    setup_response: Option<&TestResponse>,
) {
    let log = log.new(o!("url" => endpoint.url));
    info!(log, "test: begin endpoint");

    // When the user is not authenticated, failing any authz check results in a
    // "401 Unauthorized" status code.
    let unauthn_status = StatusCode::UNAUTHORIZED;

    // Determine the expected status code for authenticated, unauthorized
    // requests, based on the endpoint's visibility.
    let unauthz_status = match endpoint.visibility {
        Visibility::Public => StatusCode::FORBIDDEN,
        Visibility::Protected => StatusCode::NOT_FOUND,
    };

    // For routes with an id param, replace the id param with the setup response
    // if present.
    let uri = if endpoint.url.contains("{id}") {
        match setup_response {
            Some(response) => endpoint.url.replace(
                "{id}",
                response.parsed_body::<IdMetadata>().unwrap().id.as_str(),
            ),
            None => endpoint
                .url
                .replace("{id}", "00000000-0000-0000-0000-000000000000"),
        }
    } else {
        endpoint.url.to_string()
    };

    // Make one GET request as an authorized user to make sure we get a "200 OK"
    // response.  Otherwise, the test might later succeed by coincidence.  We
    // might find a 404 because of something that actually doesn't exist rather
    // than something that's just hidden from unauthorized users.
    let get_allowed = endpoint.allowed_methods.iter().find(|allowed| {
        matches!(
            allowed,
            AllowedMethod::Get
                | AllowedMethod::GetUnimplemented
                | AllowedMethod::GetVolatile
                | AllowedMethod::GetWebsocket
        )
    });
    let resource_before = match get_allowed {
        Some(AllowedMethod::Get) => {
            info!(log, "test: privileged GET");
            record_operation(WhichTest::PrivilegedGet(Some(
                &http::StatusCode::OK,
            )));
            Some(
                NexusRequest::object_get(client, uri.as_str())
                    .authn_as(AuthnMode::PrivilegedUser)
                    .execute()
                    .await
                    .unwrap_or_else(|e| panic!("Failed to GET: {uri}: {e}"))
                    .parsed_body::<serde_json::Value>()
                    .unwrap(),
            )
        }
        Some(AllowedMethod::GetUnimplemented) => {
            info!(log, "test: privileged GET (unimplemented)");
            let expected_status = http::StatusCode::INTERNAL_SERVER_ERROR;
            record_operation(WhichTest::PrivilegedGet(Some(&expected_status)));
            NexusRequest::expect_failure(
                client,
                expected_status,
                http::Method::GET,
                uri.as_str(),
            )
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap();
            None
        }
        Some(AllowedMethod::GetVolatile) => {
            // Same thing as `Get`, but avoid returning the output to prevent
            // the resource change detection ahead.
            info!(log, "test: privileged GET (volatile output)");
            record_operation(WhichTest::PrivilegedGet(Some(
                &http::StatusCode::OK,
            )));
            NexusRequest::object_get(client, uri.as_str())
                .authn_as(AuthnMode::PrivilegedUser)
                .execute()
                .await
                .unwrap_or_else(|e| panic!("Failed to GET: {uri}: {e}"))
                .parsed_body::<serde_json::Value>()
                .unwrap();
            None
        }
        Some(AllowedMethod::GetWebsocket) => {
            info!(log, "test: privileged GET WebSocket");
            record_operation(WhichTest::PrivilegedGet(Some(
                &http::StatusCode::SWITCHING_PROTOCOLS,
            )));
            NexusRequest::object_get(client, uri.as_str())
                .authn_as(AuthnMode::PrivilegedUser)
                .websocket_handshake()
                .execute()
                .await
                .unwrap();
            None
        }
        Some(_) => unimplemented!(),
        None => {
            warn!(log, "test: skipping privileged GET (method not allowed)");
            record_operation(WhichTest::PrivilegedGet(None));
            None
        }
    };

    print!(" ");

    // For each of the HTTP methods we use in the API as well as TRACE, we'll
    // make several requests to this URL and verify the results.
    let methods =
        [Method::GET, Method::PUT, Method::POST, Method::DELETE, Method::TRACE];
    for method in methods {
        let allowed = endpoint
            .allowed_methods
            .iter()
            .find(|allowed| method == *allowed.http_method());

        let body = allowed.and_then(|a| a.body()).cloned();

        // First, make an authenticated, unauthorized request.
        info!(log, "test: authenticated, unauthorized"; "method" => ?method);

        // This test only verifies the behavior of endpoints that a user
        // *doesn't* have access to.  Look at what kind of access is expected,
        // plus what we're trying to do, and decide whether to test it.
        let do_test_unprivileged = match (endpoint.unprivileged_access, &method)
        {
            (UnprivilegedAccess::Full, _) => false,
            (UnprivilegedAccess::ReadOnly, &Method::GET) => false,
            (UnprivilegedAccess::ReadOnly, _) => true,
            (UnprivilegedAccess::None, _) => true,
        };

        if do_test_unprivileged {
            let expected_status = match allowed {
                Some(_) => unauthz_status,
                None => StatusCode::METHOD_NOT_ALLOWED,
            };
            let mut request = NexusRequest::new(
                RequestBuilder::new(client, method.clone(), &uri)
                    .body(body.as_ref())
                    .expect_status(Some(expected_status)),
            )
            .authn_as(AuthnMode::UnprivilegedUser);
            if let Some(&AllowedMethod::GetWebsocket) = allowed {
                request = request.websocket_handshake();
            }
            let response = request.execute().await.unwrap_or_else(|e| {
                panic!("Failed making {method} request to {uri}: {e}")
            });
            verify_response(&response);
            record_operation(WhichTest::Unprivileged(&expected_status));
        } else {
            // "This door is opened elsewhere."
            print!("-");
        }

        // Next, make an unauthenticated request.
        info!(log, "test: unauthenticated"; "method" => ?method);
        let expected_status = match allowed {
            Some(_) => unauthn_status,
            None => StatusCode::METHOD_NOT_ALLOWED,
        };
        let mut request =
            RequestBuilder::new(client, method.clone(), uri.as_str())
                .body(body.as_ref())
                .expect_status(Some(expected_status));
        if let Some(&AllowedMethod::GetWebsocket) = allowed {
            request = request.expect_websocket_handshake();
        }
        let response = request.execute().await.unwrap();
        verify_response(&response);
        record_operation(WhichTest::Unauthenticated(&expected_status));

        // Now try a few requests with bogus credentials.  We should get the
        // same error as if we were unauthenticated.  This is sort of duplicated
        // by a test in test_authn_http() (which tests the authentication system
        // in general, outside the context of Nexus).  This version is an
        // end-to-end test.
        let expected_status = match allowed {
            // The 401 that you get for authentication failure overrides a 404
            // that you might get if you were authenticated but couldn't see the
            // resource in question.  That is, you should always see a 401 if
            // you fail to authenticate, whether or not the resource exists.
            Some(_) => StatusCode::UNAUTHORIZED,
            None => StatusCode::METHOD_NOT_ALLOWED,
        };

        // First, try a syntactically valid authn header for a non-existent
        // actor.
        info!(log, "test: bogus creds: bad actor"; "method" => ?method);
        let bad_actor_authn_header = &spoof::SPOOF_HEADER_BAD_ACTOR;
        let mut request =
            RequestBuilder::new(client, method.clone(), uri.as_str())
                .body(body.as_ref())
                .expect_status(Some(expected_status))
                .header(
                    &http::header::AUTHORIZATION,
                    bad_actor_authn_header.0.encode(),
                );
        if let Some(&AllowedMethod::GetWebsocket) = allowed {
            request = request.expect_websocket_handshake();
        }
        let response = request.execute().await.unwrap();
        verify_response(&response);
        record_operation(WhichTest::UnknownUser(&expected_status));

        // Now try a syntactically invalid authn header.
        info!(log, "test: bogus creds: bad cred syntax"; "method" => ?method);
        let bad_creds_authn_header = &spoof::SPOOF_HEADER_BAD_CREDS;
        let mut request =
            RequestBuilder::new(client, method.clone(), uri.as_str())
                .body(body.as_ref())
                .expect_status(Some(expected_status))
                .header(
                    &http::header::AUTHORIZATION,
                    bad_creds_authn_header.0.encode(),
                );
        if let Some(&AllowedMethod::GetWebsocket) = allowed {
            request = request.expect_websocket_handshake();
        }
        let response = request.execute().await.unwrap();
        verify_response(&response);
        record_operation(WhichTest::InvalidHeader(&expected_status));

        print!(" ");
    }

    // If we fetched the resource earlier, fetch it again and check the state.
    // We're trying to catch cases where an endpoint correctly returns an error
    // but still applied the result.
    //
    // This might seem gratuitous but it's an important check for resources like
    // disk attachment and detachment, where Nexus reaches out to the Sled Agent
    // before making a database change.  If Nexus only authorized the request at
    // the database query (as is our current emphasis), we could wind up making
    // the change to the system even for unauthorized users (and still returning
    // an "unauthorized" error)!
    // TODO-coverage It would be good to check the ETag here as well, once we
    // provide one.
    info!(log, "test: compare current resource content with earlier");
    if let Some(resource_before) = resource_before {
        let resource_after: serde_json::Value =
            NexusRequest::object_get(client, uri.as_str())
                .authn_as(AuthnMode::PrivilegedUser)
                .execute()
                .await
                .unwrap()
                .parsed_body()
                .unwrap();
        assert_eq!(
            resource_before, resource_after,
            "resource changed after making a bunch of failed requests"
        );
        record_operation(WhichTest::PrivilegedGetCheck(Some(
            &http::StatusCode::OK,
        )));
    } else {
        record_operation(WhichTest::PrivilegedGetCheck(None));
    }

    println!("  {}", endpoint.url);
}

/// Verifies the body of an HTTP response for status codes 401, 403, 404, or 405
fn verify_response(response: &TestResponse) {
    if response.status == StatusCode::SWITCHING_PROTOCOLS {
        // websocket handshake. avoid trying to parse absent body as json.
        return;
    }
    let error: HttpErrorResponseBody = response.parsed_body().unwrap();
    match response.status {
        StatusCode::UNAUTHORIZED => {
            assert_eq!(error.error_code.unwrap(), "Unauthorized");
            assert_eq!(error.message, "credentials missing or invalid");
        }
        StatusCode::FORBIDDEN => {
            assert_eq!(error.error_code.unwrap(), "Forbidden");
            assert_eq!(error.message, "Forbidden");
        }
        StatusCode::NOT_FOUND => {
            assert_eq!(error.error_code.unwrap(), "ObjectNotFound");
            assert!(error.message.starts_with("not found: "));
            assert!(
                error.message.contains(" with name \"")
                    || error.message.contains(" with id \"")
            );
            assert!(error.message.ends_with('\"'));
        }
        StatusCode::METHOD_NOT_ALLOWED => {
            assert!(error.error_code.is_none());
            assert_eq!(error.message, "Method Not Allowed");
        }
        _ => unimplemented!(),
    }
}

/// Describes the tests run by [`verify_endpoint()`].
enum WhichTest<'a> {
    PrivilegedGet(Option<&'a http::StatusCode>),
    Unprivileged(&'a http::StatusCode),
    Unauthenticated(&'a http::StatusCode),
    UnknownUser(&'a http::StatusCode),
    InvalidHeader(&'a http::StatusCode),
    PrivilegedGetCheck(Option<&'a http::StatusCode>),
}

/// Prints one cell of the giant summary table describing the successful result
/// of one HTTP request.
fn record_operation(whichtest: WhichTest<'_>) {
    // Extract the status code for the test.
    let status_code = match whichtest {
        WhichTest::PrivilegedGet(s) | WhichTest::PrivilegedGetCheck(s) => s,
        WhichTest::Unprivileged(s) => Some(s),
        WhichTest::Unauthenticated(s) => Some(s),
        WhichTest::UnknownUser(s) => Some(s),
        WhichTest::InvalidHeader(s) => Some(s),
    };

    // We'll print out the third digit of the HTTP status code.
    let c = match status_code {
        Some(s) => s.as_str().chars().nth(2).unwrap(),
        None => '-',
    };

    // We only get here for successful results, so they're all green.  You might
    // think the color is pointless, but it does help the reader make sense of
    // the mess of numbers that shows up in the table for the different response
    // codes.
    let t = term::stdout();
    if let Some(mut term) = t {
        // We just want to write one green character to stdout.  But we also
        // want it to be captured by the test runner like people usually expect
        // when they haven't passed "--nocapture".  The test runner only
        // captures output from the `print!` family of macros, not all writes to
        // stdout.  So we write the formatting control character, flush that (to
        // make sure it gets emitted before our character), use print for our
        // character, reset the terminal, then flush that.
        //
        // Note that this likely still writes the color-changing control
        // characters to the real stdout, even without "--nocapture".  That
        // sucks, but at least you don't see them.
        //
        // We also don't unwrap() the results of printing control codes
        // in case the terminal doesn't support them.
        let _ = term.fg(term::color::GREEN);
        let _ = term.flush();
        print!("{}", c);
        let _ = term.reset();
        let _ = term.flush();
    } else {
        print!("{}", c);
    }
}
