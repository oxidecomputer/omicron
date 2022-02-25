// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Verify the behavior of API endpoints when hit by unauthenticated and
//! unauthorized users

use dropshot::test_util::ClientTestContext;
use dropshot::HttpErrorResponseBody;
use headers::authorization::Credentials;
use http::method::Method;
use http::StatusCode;
use lazy_static::lazy_static;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::http_testing::TestResponse;
use nexus_test_utils::resource_helpers::DiskTest;
use nexus_test_utils::ControlPlaneTestContext;
use nexus_test_utils_macros::nexus_test;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::IdentityMetadataUpdateParams;
use omicron_common::api::external::InstanceCpuCount;
use omicron_common::api::external::Name;
use omicron_nexus::authn;
use omicron_nexus::authn::external::spoof;
use omicron_nexus::external_api::params;

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
// * It'd be nice to verify that all the endpoints listed here are within the
//   OpenAPI spec.
// * It'd be nice to produce a list of endpoints from the OpenAPI spec that are
//   not checked here.  We could put this into an expectorate file and make sure
//   that we don't add new unchecked endpoints.
// * When we finish authz, maybe the hardcoded information here can come instead
//   from the OpenAPI spec?
#[nexus_test]
async fn test_unauthorized(cptestctx: &ControlPlaneTestContext) {
    DiskTest::new(cptestctx).await;
    let client = &cptestctx.external_client;
    let log = &cptestctx.logctx.log;

    // Create test data.
    info!(log, "setting up resource hierarchy");
    for request in &*SETUP_REQUESTS {
        NexusRequest::objects_post(client, request.url, &request.body)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap();
    }

    // Verify the hardcoded endpoints.
    info!(log, "verifying endpoints");
    for endpoint in &*VERIFY_ENDPOINTS {
        verify_endpoint(&log, client, endpoint).await;
    }
}

//
// SETUP PHASE
//

/// Describes a request made during the setup phase to create a resource that
/// we'll use later in the verification phase
///
/// The setup phase takes a list of `SetupReq` structs and issues `POST`
/// requests to each one's `url` with the specific `body`.
struct SetupReq {
    /// url to send the `POST` to
    url: &'static str,
    /// body of the `POST` request
    body: serde_json::Value,
}

lazy_static! {
    /// List of requests to execute at setup time
    static ref SETUP_REQUESTS: Vec<SetupReq> = vec![
        // Create an Organization
        SetupReq {
            url: "/organizations",
            body: serde_json::to_value(&*DEMO_ORG_CREATE).unwrap()
        },
        // Create a Project in the Organization
        SetupReq {
            url: &*DEMO_ORG_PROJECTS_URL,
            body: serde_json::to_value(&*DEMO_PROJECT_CREATE).unwrap(),
        },
        // Create a Disk in the Project
        SetupReq {
            url: &*DEMO_PROJECT_URL_DISKS,
            body: serde_json::to_value(&*DEMO_DISK_CREATE).unwrap(),
        },
        // Create an Instance in the Project
        SetupReq {
            url: &*DEMO_PROJECT_URL_INSTANCES,
            body: serde_json::to_value(&*DEMO_INSTANCE_CREATE).unwrap(),
        },
    ];

    // Organization used for testing
    static ref DEMO_ORG_NAME: Name = "demo-org".parse().unwrap();
    static ref DEMO_ORG_URL: String =
        format!("/organizations/{}", *DEMO_ORG_NAME);
    static ref DEMO_ORG_PROJECTS_URL: String =
        format!("{}/projects", *DEMO_ORG_URL);
    static ref DEMO_ORG_CREATE: params::OrganizationCreate =
        params::OrganizationCreate {
            identity: IdentityMetadataCreateParams {
                name: DEMO_ORG_NAME.clone(),
                description: "".parse().unwrap(),
            },
        };

    // Project used for testing
    static ref DEMO_PROJECT_NAME: Name = "demo-project".parse().unwrap();
    static ref DEMO_PROJECT_URL: String =
        format!("{}/{}", *DEMO_ORG_PROJECTS_URL, *DEMO_PROJECT_NAME);
    static ref DEMO_PROJECT_URL_DISKS: String =
        format!("{}/disks", *DEMO_PROJECT_URL);
    static ref DEMO_PROJECT_URL_INSTANCES: String =
        format!("{}/instances", *DEMO_PROJECT_URL);
    static ref DEMO_PROJECT_CREATE: params::ProjectCreate =
        params::ProjectCreate {
            identity: IdentityMetadataCreateParams {
                name: DEMO_PROJECT_NAME.clone(),
                description: "".parse().unwrap(),
            },
        };

    // Disk used for testing
    static ref DEMO_DISK_NAME: Name = "demo-disk".parse().unwrap();
    static ref DEMO_DISK_URL: String =
        format!("{}/{}", *DEMO_PROJECT_URL_DISKS, *DEMO_DISK_NAME);
    static ref DEMO_DISK_CREATE: params::DiskCreate =
        params::DiskCreate {
            identity: IdentityMetadataCreateParams {
                name: DEMO_DISK_NAME.clone(),
                description: "".parse().unwrap(),
            },
            snapshot_id: None,
            size: ByteCount::from_gibibytes_u32(16),
        };

    // Instance used for testing
    static ref DEMO_INSTANCE_NAME: Name = "demo-instance".parse().unwrap();
    static ref DEMO_INSTANCE_URL: String =
        format!("{}/{}", *DEMO_PROJECT_URL_INSTANCES, *DEMO_INSTANCE_NAME);
    static ref DEMO_INSTANCE_START_URL: String =
        format!("{}/start", *DEMO_INSTANCE_URL);
    static ref DEMO_INSTANCE_STOP_URL: String =
        format!("{}/stop", *DEMO_INSTANCE_URL);
    static ref DEMO_INSTANCE_REBOOT_URL: String =
        format!("{}/reboot", *DEMO_INSTANCE_URL);
    static ref DEMO_INSTANCE_MIGRATE_URL: String =
        format!("{}/migrate", *DEMO_INSTANCE_URL);
    static ref DEMO_INSTANCE_DISKS_URL: String =
        format!("{}/disks", *DEMO_INSTANCE_URL);
    static ref DEMO_INSTANCE_DISKS_ATTACH_URL: String =
        format!("{}/attach", *DEMO_INSTANCE_DISKS_URL);
    static ref DEMO_INSTANCE_DISKS_DETACH_URL: String =
        format!("{}/detach", *DEMO_INSTANCE_DISKS_URL);
    static ref DEMO_INSTANCE_CREATE: params::InstanceCreate =
        params::InstanceCreate {
            identity: IdentityMetadataCreateParams {
                name: DEMO_INSTANCE_NAME.clone(),
                description: "".parse().unwrap(),
            },
            ncpus: InstanceCpuCount(1),
            memory: ByteCount::from_gibibytes_u32(16),
            hostname: String::from("demo-instance"),
        };
}

//
// VERIFY PHASE
//

/// Describes an API endpoint to be verified
struct VerifyEndpoint {
    /// URL path for the HTTP resource to test
    ///
    /// Note that we might talk about the "GET organization" endpoint, and might
    /// write that "GET /organizations/{organization_name}".  But the URL here
    /// is for a specific HTTP resource, so it would look like
    /// "/organizations/demo-org" rather than
    /// "/organizations/{organization_name}".
    url: &'static str,

    /// Specifies whether an HTTP resource handled by this endpoint is visible
    /// to unauthenticated or unauthorized users
    ///
    /// If it's [`Visibility::Public`] (like "/organizations"), unauthorized
    /// users can expect to get back a 401 or 403 when they attempt to access
    /// it.  If it's [`Visibility::Protected`] (like a specific Organization),
    /// unauthorized users will get a 404.
    visibility: Visibility,

    /// Specifies what HTTP methods are supported for this HTTP resource
    ///
    /// The test runner tests a variety of HTTP methods.  For each method, if
    /// it's not in this list, we expect a 405 "Method Not Allowed" response.
    /// For `PUT` and `POST`, the item in `allowed_methods` also contains the
    /// contents of the body to send with the `PUT` or `POST` request.  This
    /// should be valid input for the endpoint.  Otherwise, Nexus could choose
    /// to fail with a 400-level validation error, which would obscure the
    /// authn/authz error we're looking for.
    allowed_methods: Vec<AllowedMethod>,
}

/// Describes the visibility of an HTTP resource
enum Visibility {
    /// All users can see the resource (including unauthenticated or
    /// unauthorized users)
    ///
    /// "/organizations" is Public, for example.
    Public,

    /// Only users with certain privileges can see this endpoint
    ///
    /// "/organizations/demo-org" is not public, for example.
    Protected,
}

/// Describes an HTTP method supported by a particular API endpoint
enum AllowedMethod {
    /// HTTP "DELETE" method
    Delete,
    /// HTTP "GET" method
    Get,
    /// HTTP "POST" method, with sample input (which should be valid input for
    /// this endpoint)
    Post(serde_json::Value),
    /// HTTP "PUT" method, with sample input (which should be valid input for
    /// this endpoint)
    Put(serde_json::Value),
}

impl AllowedMethod {
    /// Returns the [`http::Method`] used to make a request for this HTTP method
    fn http_method(&self) -> &'static http::Method {
        match self {
            AllowedMethod::Delete => &Method::DELETE,
            AllowedMethod::Get => &Method::GET,
            AllowedMethod::Post(_) => &Method::POST,
            AllowedMethod::Put(_) => &Method::PUT,
        }
    }

    /// Returns a JSON value suitable for use as the request body when making a
    /// request to a specific endpoint using this HTTP method
    ///
    /// If this returns `None`, the request body should be empty.
    fn body(&self) -> Option<&serde_json::Value> {
        match self {
            AllowedMethod::Delete | AllowedMethod::Get => None,
            AllowedMethod::Post(body) => Some(&body),
            AllowedMethod::Put(body) => Some(&body),
        }
    }
}

lazy_static! {
    static ref URL_USERS_DB_INIT: String =
        format!("/users/{}", authn::USER_DB_INIT.name);

    /// List of endpoints to be verified
    static ref VERIFY_ENDPOINTS: Vec<VerifyEndpoint> = vec![
        /* Organizations */

        VerifyEndpoint {
            url: "/organizations",
            visibility: Visibility::Public,
            allowed_methods: vec![
                AllowedMethod::Get,
                AllowedMethod::Post(
                    serde_json::to_value(&*DEMO_ORG_CREATE).unwrap()
                )
            ],
        },
        VerifyEndpoint {
            url: &*DEMO_ORG_URL,
            visibility: Visibility::Protected,
            allowed_methods: vec![
                AllowedMethod::Get,
                AllowedMethod::Delete,
                AllowedMethod::Put(
                    serde_json::to_value(&params::OrganizationUpdate {
                        identity: IdentityMetadataUpdateParams {
                            name: None,
                            description: Some("different".to_string())
                        }
                    }).unwrap()
                ),
            ],
        },

        /* Projects */

        // TODO-security TODO-correctness One thing that's a little strange
        // here: we currently return a 404 if you attempt to create a Project
        // inside an Organization and you're not authorized to do that.  In an
        // ideal world, we'd return a 403 if you can _see_ the Organization and
        // a 404 if not.  But we don't really know if you should be able to see
        // the Organization.  Right now, the only real way to tell that is if
        // you have permissions on anything _inside_ the Organization, which is
        // incredibly expensive to determine in general.
        VerifyEndpoint {
            url: &*DEMO_ORG_PROJECTS_URL,
            visibility: Visibility::Protected,
            allowed_methods: vec![
                AllowedMethod::Get,
                AllowedMethod::Post(
                    serde_json::to_value(&*DEMO_PROJECT_CREATE).unwrap()
                ),
            ],
        },
        VerifyEndpoint {
            url: &*DEMO_PROJECT_URL,
            visibility: Visibility::Protected,
            allowed_methods: vec![
                AllowedMethod::Get,
                AllowedMethod::Delete,
                AllowedMethod::Put(
                    serde_json::to_value(params::ProjectUpdate{
                        identity: IdentityMetadataUpdateParams {
                            name: None,
                            description: Some("different".to_string())
                        },
                    }).unwrap()
                ),
            ],
        },

        /* Disks */

        VerifyEndpoint {
            url: &*DEMO_PROJECT_URL_DISKS,
            visibility: Visibility::Protected,
            allowed_methods: vec![
                AllowedMethod::Get,
                AllowedMethod::Post(
                    serde_json::to_value(&*DEMO_DISK_CREATE).unwrap()
                ),
            ],
        },

        VerifyEndpoint {
            url: &*DEMO_DISK_URL,
            visibility: Visibility::Protected,
            allowed_methods: vec![
                AllowedMethod::Get,
                AllowedMethod::Delete,
            ],
        },

        VerifyEndpoint {
            url: &*DEMO_INSTANCE_DISKS_ATTACH_URL,
            visibility: Visibility::Protected,
            allowed_methods: vec![
                AllowedMethod::Post(
                    serde_json::to_value(params::DiskIdentifier {
                        disk: DEMO_DISK_NAME.clone()
                    }).unwrap()
                )
            ],
        },
        VerifyEndpoint {
            url: &*DEMO_INSTANCE_DISKS_DETACH_URL,
            visibility: Visibility::Protected,
            allowed_methods: vec![
                AllowedMethod::Post(
                    serde_json::to_value(params::DiskIdentifier {
                        disk: DEMO_DISK_NAME.clone()
                    }).unwrap()
                )
            ],
        },

        /* Instances */
        VerifyEndpoint {
            url: &*DEMO_PROJECT_URL_INSTANCES,
            visibility: Visibility::Protected,
            allowed_methods: vec![
                AllowedMethod::Get,
                AllowedMethod::Post(
                    serde_json::to_value(&*DEMO_INSTANCE_CREATE).unwrap()
                ),
            ],
        },

        VerifyEndpoint {
            url: &*DEMO_INSTANCE_URL,
            visibility: Visibility::Protected,
            allowed_methods: vec![
                AllowedMethod::Get,
                AllowedMethod::Delete,
            ],
        },

        VerifyEndpoint {
            url: &*DEMO_INSTANCE_START_URL,
            visibility: Visibility::Protected,
            allowed_methods: vec![
                AllowedMethod::Post(serde_json::Value::Null)
            ],
        },
        VerifyEndpoint {
            url: &*DEMO_INSTANCE_STOP_URL,
            visibility: Visibility::Protected,
            allowed_methods: vec![
                AllowedMethod::Post(serde_json::Value::Null)
            ],
        },
        VerifyEndpoint {
            url: &*DEMO_INSTANCE_REBOOT_URL,
            visibility: Visibility::Protected,
            allowed_methods: vec![
                AllowedMethod::Post(serde_json::Value::Null)
            ],
        },
        VerifyEndpoint {
            url: &*DEMO_INSTANCE_MIGRATE_URL,
            visibility: Visibility::Protected,
            allowed_methods: vec![
                AllowedMethod::Post(serde_json::to_value(
                    params::InstanceMigrate {
                        dst_sled_uuid: uuid::Uuid::new_v4(),
                    }
                ).unwrap()),
            ],
        },

        /* IAM */

        VerifyEndpoint {
            url: "/roles",
            visibility: Visibility::Public,
            allowed_methods: vec![AllowedMethod::Get],
        },
        VerifyEndpoint {
            url: "/roles/fleet.admin",
            visibility: Visibility::Protected,
            allowed_methods: vec![AllowedMethod::Get],
        },

        VerifyEndpoint {
            url: "/users",
            visibility: Visibility::Public,
            allowed_methods: vec![AllowedMethod::Get],
        },
        VerifyEndpoint {
            url: &*URL_USERS_DB_INIT,
            visibility: Visibility::Protected,
            allowed_methods: vec![AllowedMethod::Get],
        },
    ];
}

/// Verifies a single API endpoint, described with `endpoint`
///
/// (Technically, a single `VerifyEndpoint` struct describes an HTTP resource,
/// like "/organizations".  There are several API endpoints there, like "GET
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
///   We assume that everybody is allowed to know that "/organizations" exists.
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
) {
    let log = log.new(o!("url" => endpoint.url));
    info!(log, "test: begin endpoint");

    // Determine the expected status code for unauthenticated requests, based on
    // the endpoint's visibility.
    let unauthn_status = match endpoint.visibility {
        Visibility::Public => StatusCode::UNAUTHORIZED,
        Visibility::Protected => StatusCode::NOT_FOUND,
    };

    // Determine the expected status code for authenticated, unauthorized
    // requests, based on the endpoint's visibility.
    let unauthz_status = match endpoint.visibility {
        Visibility::Public => StatusCode::FORBIDDEN,
        Visibility::Protected => StatusCode::NOT_FOUND,
    };

    // Make one GET request as an authorized user to make sure we get a "200 OK"
    // response.  Otherwise, the test might later succeed by coincidence.  We
    // might find a 404 because of something that actually doesn't exist rather
    // than something that's just hidden from unauthorized users.
    let get_allowed = endpoint
        .allowed_methods
        .iter()
        .any(|allowed| allowed.http_method() == Method::GET);
    let resource_before: Option<serde_json::Value> = if get_allowed {
        info!(log, "test: privileged GET");
        Some(
            NexusRequest::object_get(client, endpoint.url)
                .authn_as(AuthnMode::PrivilegedUser)
                .execute()
                .await
                .unwrap()
                .parsed_body()
                .unwrap(),
        )
    } else {
        warn!(log, "test: skipping privileged GET (method not allowed)");
        None
    };

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
        let expected_status = match allowed {
            Some(_) => unauthz_status,
            None => StatusCode::METHOD_NOT_ALLOWED,
        };
        let response = NexusRequest::new(
            RequestBuilder::new(client, method.clone(), endpoint.url)
                .body(body.as_ref())
                .expect_status(Some(expected_status)),
        )
        .authn_as(AuthnMode::UnprivilegedUser)
        .execute()
        .await
        .unwrap();
        verify_response(&response);

        // Next, make an unauthenticated request.
        info!(log, "test: unauthenticated"; "method" => ?method);
        let expected_status = match allowed {
            Some(_) => unauthn_status,
            None => StatusCode::METHOD_NOT_ALLOWED,
        };
        let response =
            RequestBuilder::new(client, method.clone(), endpoint.url)
                .body(body.as_ref())
                .expect_status(Some(expected_status))
                .execute()
                .await
                .unwrap();
        verify_response(&response);

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
        let response =
            RequestBuilder::new(client, method.clone(), endpoint.url)
                .body(body.as_ref())
                .expect_status(Some(expected_status))
                .header(
                    &http::header::AUTHORIZATION,
                    bad_actor_authn_header.0.encode(),
                )
                .execute()
                .await
                .unwrap();
        verify_response(&response);

        // Now try a syntactically invalid authn header.
        info!(log, "test: bogus creds: bad cred syntax"; "method" => ?method);
        let bad_creds_authn_header = &spoof::SPOOF_HEADER_BAD_CREDS;
        let response =
            RequestBuilder::new(client, method.clone(), endpoint.url)
                .body(body.as_ref())
                .expect_status(Some(expected_status))
                .header(
                    &http::header::AUTHORIZATION,
                    bad_creds_authn_header.0.encode(),
                )
                .execute()
                .await
                .unwrap();
        verify_response(&response);
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
            NexusRequest::object_get(client, endpoint.url)
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
    }
}

/// Verifies the body of an HTTP response for status codes 401, 403, 404, or 405
fn verify_response(response: &TestResponse) {
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
            assert!(error.message.contains(" with name \""));
            assert!(error.message.ends_with("\""));
        }
        StatusCode::METHOD_NOT_ALLOWED => {
            assert!(error.error_code.is_none());
            assert_eq!(error.message, "Method Not Allowed");
        }
        _ => unimplemented!(),
    }
}
