// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! This test hits authorization-protected endpoints with unauthenticated and
//! unauthorized users to make sure we get the expected behavior (generally:
//! 401, 403, or 404).

// TODO-coverage
// * It would be good to add a built-in test user that can read everything in
//   the world and use that to exercise 404 vs. 401/403 behavior.
// * It'd be nice to verify that all the endpoints listed here are within the
//   OpenAPI spec.
// * It'd be nice to produce a list of endpoints from the OpenAPI spec that are
//   not checked here.  We could put this into an expectorate file and make sure
//   that we don't add new unchecked endpoints.
// * When we finish authz, maybe the hardcoded information here can come instead
//   from the OpenAPI spec?

use dropshot::test_util::ClientTestContext;
use dropshot::HttpErrorResponseBody;
use http::method::Method;
use http::StatusCode;
use lazy_static::lazy_static;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::http_testing::TestResponse;
use nexus_test_utils::ControlPlaneTestContext;
use nexus_test_utils_macros::nexus_test;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::IdentityMetadataUpdateParams;
use omicron_nexus::authn;
use omicron_nexus::authn::external::spoof::HTTP_HEADER_OXIDE_AUTHN_SPOOF;
use omicron_nexus::external_api::params;

#[nexus_test]
async fn test_unauthorized(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let log = &cptestctx.logctx.log;

    // Create test data.
    for request in &*SETUP_REQUESTS {
        NexusRequest::objects_post(client, request.url, &request.body)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap();
    }

    for endpoint in &*VERIFY_ENDPOINTS {
        verify_endpoint(&log, client, endpoint).await;
    }
}

struct SetupReq {
    url: &'static str,
    body: serde_json::Value,
}

lazy_static! {
    static ref DEMO_ORG_CREATE: params::OrganizationCreate =
        params::OrganizationCreate {
            identity: IdentityMetadataCreateParams {
                name: "demo-org".parse().unwrap(),
                description: "".parse().unwrap(),
            },
        };
    static ref DEMO_PROJECT_CREATE: params::ProjectCreate =
        params::ProjectCreate {
            identity: IdentityMetadataCreateParams {
                name: "demo-project".parse().unwrap(),
                description: "".parse().unwrap(),
            },
        };
    static ref SETUP_REQUESTS: Vec<SetupReq> = vec![
        SetupReq {
            url: "/organizations",
            body: serde_json::to_value(&*DEMO_ORG_CREATE).unwrap()
        },
        SetupReq {
            url: "/organizations/demo-org/projects",
            body: serde_json::to_value(&*DEMO_PROJECT_CREATE).unwrap(),
        },
    ];
}

enum Visibility {
    Everyone,
    Protected,
}

enum AllowedMethod {
    Delete,
    Get,
    Post(serde_json::Value),
    Put(serde_json::Value),
}

impl AllowedMethod {
    fn http_method(&self) -> &'static http::Method {
        match self {
            AllowedMethod::Delete => &Method::DELETE,
            AllowedMethod::Get => &Method::GET,
            AllowedMethod::Post(_) => &Method::POST,
            AllowedMethod::Put(_) => &Method::PUT,
        }
    }

    fn body(&self) -> Option<&serde_json::Value> {
        match self {
            AllowedMethod::Delete | AllowedMethod::Get => None,
            AllowedMethod::Post(body) => Some(&body),
            AllowedMethod::Put(body) => Some(&body),
        }
    }
}

struct VerifyEndpoint {
    url: &'static str,
    visibility: Visibility,
    allowed_methods: Vec<AllowedMethod>,
}

lazy_static! {
    static ref URL_USERS_DB_INIT: String =
        format!("/users/{}", authn::USER_DB_INIT.name);
    static ref VERIFY_ENDPOINTS: Vec<VerifyEndpoint> = vec![
        VerifyEndpoint {
            url: "/organizations",
            visibility: Visibility::Everyone,
            allowed_methods: vec![
                AllowedMethod::Get,
                AllowedMethod::Post(
                    serde_json::to_value(&*DEMO_ORG_CREATE).unwrap()
                )
            ],
        },
        VerifyEndpoint {
            url: "/organizations/demo-org",
            visibility: Visibility::Protected,
            allowed_methods: vec![
                AllowedMethod::Get,
                AllowedMethod::Delete,
                AllowedMethod::Put(
                    serde_json::to_value(&params::OrganizationUpdate {
                        identity: IdentityMetadataUpdateParams {
                            name: None,
                            description: None,
                        }
                    }).unwrap()
                ),
            ],
        },

        // TODO-security TODO-correctness One thing that's a little strange
        // here: we currently return a 404 if you attempt to create a Project
        // inside an Organization and you're not authorized to do that.  In an
        // ideal world, we'd return a 403 if you can _see_ the Organization and
        // a 404 if not.  But we don't really know if you should be able to see
        // the Organization.  Right now, the only real way to tell that is if
        // you have permissions on anything _inside_ the Organization, which is
        // incredibly expensive to determine in general.
        VerifyEndpoint {
            url: "/organizations/demo-org/projects",
            visibility: Visibility::Protected,
            allowed_methods: vec![
                AllowedMethod::Get,
                AllowedMethod::Post(
                    serde_json::to_value(&*DEMO_PROJECT_CREATE).unwrap()
                ),
            ],
        },
        VerifyEndpoint {
            url: "/organizations/demo-org/projects/demo-project",
            visibility: Visibility::Protected,
            allowed_methods: vec![
                AllowedMethod::Get,
                AllowedMethod::Delete,
                AllowedMethod::Put(
                    serde_json::to_value(params::ProjectUpdate{
                        identity: IdentityMetadataUpdateParams {
                            name: None,
                            description: None,
                        },
                    }).unwrap()
                ),
            ],
        },
        VerifyEndpoint {
            url: "/roles",
            visibility: Visibility::Everyone,
            allowed_methods: vec![AllowedMethod::Get],
        },
        VerifyEndpoint {
            url: "/roles/fleet.admin",
            visibility: Visibility::Protected,
            allowed_methods: vec![AllowedMethod::Get],
        },
        VerifyEndpoint {
            url: "/users",
            visibility: Visibility::Everyone,
            allowed_methods: vec![AllowedMethod::Get],
        },
        VerifyEndpoint {
            url: &*URL_USERS_DB_INIT,
            visibility: Visibility::Protected,
            allowed_methods: vec![AllowedMethod::Get],
        },

        // XXX clean up and document this test
    ];
}

async fn verify_endpoint(
    log: &slog::Logger,
    client: &ClientTestContext,
    endpoint: &VerifyEndpoint,
) {
    info!(log, "test endpoint"; "url" => endpoint.url);
    let methods =
        [Method::GET, Method::PUT, Method::POST, Method::DELETE, Method::TRACE];

    // XXX Tests that we want to run
    // privileged GET /url => always a 200
    // unauthenticated GET, DELETE /url
    //     if public, then 401; otherwise 404
    // unauthenticated PUT /url
    //     if has body
    //         if public, then 401; otherwise 404
    //     else
    //         if public, then 405; otherwise 404 (how can this possibly work
    //         XXX it'll probably be 405 and that's probably okay)
    // unauthenticated POST /url: same as unauthenticated PUT
    // unauthenticated TRACE: always 405
    //
    // unauthorized GET, DELETE /url:
    //     if public, then 403; otherwise 404
    // unauthorized PUT /url:
    //     if has body
    //      if public, then 403; otherwise 404
    //     else
    //      always 405
    //

    let unauthn_status = match endpoint.visibility {
        Visibility::Everyone => StatusCode::UNAUTHORIZED,
        Visibility::Protected => StatusCode::NOT_FOUND,
    };

    let unauthz_status = match endpoint.visibility {
        Visibility::Everyone => StatusCode::FORBIDDEN,
        Visibility::Protected => StatusCode::NOT_FOUND,
    };

    // Make one GET request as an authorized user to make sure we get a
    // "200 OK" response.  Otherwise, the test might later succeed by
    // coincidence.  We might find a 404 because of something that actually
    // doesn't exist rather than something that's just hidden from
    // unauthorized users.
    NexusRequest::object_get(client, endpoint.url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap();

    for method in methods {
        let allowed = endpoint
            .allowed_methods
            .iter()
            .find(|allowed| method == *allowed.http_method());

        let body = match method {
            // Always supply some body for POST and PUT.  If this is an allowed
            // method, then it will have a body in the structure.  Otherwise,
            // make one up.
            Method::POST | Method::PUT => allowed
                .and_then(|a| a.body())
                .cloned()
                .or_else(|| Some(serde_json::Value::String(String::new()))),
            _ => allowed.and_then(|a| a.body()).cloned(),
        };

        // First, make an authenticated, unauthorized request.
        let expected_status = if allowed.is_none() {
            StatusCode::METHOD_NOT_ALLOWED
        } else {
            unauthz_status
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

        // First, let's make an unauthenticated request.
        let expected_status = if allowed.is_none() {
            StatusCode::METHOD_NOT_ALLOWED
        } else {
            unauthn_status
        };
        let response =
            RequestBuilder::new(client, method.clone(), endpoint.url)
                .body(body.as_ref())
                .expect_status(Some(expected_status))
                .execute()
                .await
                .unwrap();
        verify_response(&response);

        // If we provide invalid credentials altogether, we should get the same
        // error as if we were unauthenticated.  This is sort of duplicated by a
        // test in test_authn_http() (which tests the authentication system in
        // general, outside the context of Nexus).  These two tests verify that
        // we've correctly integrated authn with Nexus.
        //
        // First, try a syntactically valid authn header for a non-existent
        // actor.

        let expected_status = if allowed.is_none() {
            StatusCode::METHOD_NOT_ALLOWED
        } else {
            // The 401 that you get for authentication failure overrides a 404
            // that you might get if you were authenticated but couldn't see the
            // resource in question.  That is, you should always see a 401 if
            // you fail to authenticate, whether or not the resource exists.
            StatusCode::UNAUTHORIZED
        };
        let bad_actor_authn_header = http::HeaderValue::from_str(
            omicron_nexus::authn::external::spoof::SPOOF_RESERVED_BAD_ACTOR,
        )
        .unwrap();
        let response =
            RequestBuilder::new(client, method.clone(), endpoint.url)
                .body(body.as_ref())
                .expect_status(Some(expected_status))
                .header(HTTP_HEADER_OXIDE_AUTHN_SPOOF, bad_actor_authn_header)
                .execute()
                .await
                .unwrap();
        verify_response(&response);

        // Now try a syntactically valid authn header.
        let bad_creds_authn_header = http::HeaderValue::from_str(
            omicron_nexus::authn::external::spoof::SPOOF_RESERVED_BAD_CREDS,
        )
        .unwrap();
        let response =
            RequestBuilder::new(client, method.clone(), endpoint.url)
                .body(body.as_ref())
                .expect_status(Some(expected_status))
                .header(HTTP_HEADER_OXIDE_AUTHN_SPOOF, bad_creds_authn_header)
                .execute()
                .await
                .unwrap();
        verify_response(&response);
    }
}

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
