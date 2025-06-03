// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::Context;
use camino::Utf8PathBuf;
use dropshot::ResultsPage;
use dropshot::test_util::ClientTestContext;
use http::header::HeaderName;
use http::{StatusCode, header, method::Method};
use nexus_auth::context::OpContext;
use std::env::current_dir;

use crate::integration_tests::saml::SAML_RESPONSE_IDP_DESCRIPTOR;
use base64::Engine;
use internal_dns_types::names::DNS_ZONE_EXTERNAL_TESTING;
use nexus_db_queries::authn::{USER_TEST_PRIVILEGED, USER_TEST_UNPRIVILEGED};
use nexus_db_queries::db::fixed_data::silo::DEFAULT_SILO;
use nexus_db_queries::db::identity::{Asset, Resource};
use nexus_test_utils::http_testing::{
    AuthnMode, NexusRequest, RequestBuilder, TestResponse,
};
use nexus_test_utils::resource_helpers::test_params;
use nexus_test_utils::resource_helpers::{
    create_silo, grant_iam, object_create,
};
use nexus_test_utils::{
    TEST_SUITE_PASSWORD, load_test_config, test_setup_with_config,
};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params::{self, ProjectCreate};
use nexus_types::external_api::shared::{SiloIdentityMode, SiloRole};
use nexus_types::external_api::{shared, views};
use omicron_common::api::external::{Error, IdentityMetadataCreateParams};
use omicron_sled_agent::sim;
use omicron_test_utils::dev::poll::{CondCheckError, wait_for_condition};

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

#[nexus_test]
async fn test_sessions(cptestctx: &ControlPlaneTestContext) {
    let testctx = &cptestctx.external_client;

    // logout always gives the same response whether you have a session or not
    RequestBuilder::new(&testctx, Method::POST, "/v1/logout")
        .expect_status(Some(StatusCode::NO_CONTENT))
        .expect_response_header(
            header::SET_COOKIE,
            "session=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0",
        )
        .execute()
        .await
        .expect("failed to clear cookie and 204 on logout");

    // log in and pull the token out of the header so we can use it for authed requests
    let session_token = log_in_and_extract_token(cptestctx).await;

    let project_params = ProjectCreate {
        identity: IdentityMetadataCreateParams {
            name: "my-proj".parse().unwrap(),
            description: "a project".to_string(),
        },
    };

    // hitting auth-gated API endpoint without session cookie 401s
    RequestBuilder::new(&testctx, Method::POST, "/v1/projects")
        .body(Some(&project_params))
        .expect_status(Some(StatusCode::UNAUTHORIZED))
        .execute()
        .await
        .expect("failed to 401 on unauthed API request");

    // console pages don't 401, they 302
    RequestBuilder::new(&testctx, Method::GET, "/projects/whatever")
        .expect_status(Some(StatusCode::FOUND))
        .execute()
        .await
        .expect("failed to 302 on unauthed console page request");

    // Our test uses the "unprivileged" user to make sure login/logout works
    // without other privileges.  However, they _do_ need the privilege to
    // create Organizations because we'll be testing that as a smoke test.
    // We'll remove that privilege afterwards.
    let silo_url = format!("/v1/system/silos/{}", DEFAULT_SILO.identity().name);
    let policy_url = format!("{}/policy", silo_url);
    let initial_policy: shared::Policy<SiloRole> =
        NexusRequest::object_get(testctx, &policy_url)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .expect("failed to fetch Silo policy")
            .parsed_body()
            .expect("failed to parse Silo policy");
    grant_iam(
        testctx,
        &silo_url,
        SiloRole::Collaborator,
        USER_TEST_UNPRIVILEGED.id(),
        AuthnMode::PrivilegedUser,
    )
    .await;

    // now make same requests with cookie
    RequestBuilder::new(&testctx, Method::POST, "/v1/projects")
        .header(header::COOKIE, &session_token)
        .body(Some(&project_params))
        // TODO: explicit expect_status not needed. decide whether to keep it anyway
        .expect_status(Some(StatusCode::CREATED))
        .execute()
        .await
        .expect("failed to create org with session cookie");

    RequestBuilder::new(&testctx, Method::GET, "/projects/whatever")
        .header(header::COOKIE, &session_token)
        .expect_console_asset()
        .execute()
        .await
        .expect("failed to get console page with session cookie");

    NexusRequest::object_put(testctx, &policy_url, Some(&initial_policy))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to restore Silo policy");

    // logout with an actual session should delete the session in the db
    RequestBuilder::new(&testctx, Method::POST, "/v1/logout")
        .header(header::COOKIE, &session_token)
        .expect_status(Some(StatusCode::NO_CONTENT))
        // logout also clears the cookie client-side
        .expect_response_header(
            header::SET_COOKIE,
            "session=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0",
        )
        .execute()
        .await
        .expect("failed to log out");

    // logout with a nonexistent session token does the same thing: clears it out
    RequestBuilder::new(&testctx, Method::POST, "/v1/logout")
        .header(header::COOKIE, "session=abc")
        .expect_status(Some(StatusCode::NO_CONTENT))
        // logout also clears the cookie client-side
        .expect_response_header(
            header::SET_COOKIE,
            "session=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0",
        )
        .execute()
        .await
        .expect("failed to log out");

    // now the same requests with the same session cookie should 401/302 because
    // logout also deletes the session server-side
    RequestBuilder::new(&testctx, Method::POST, "/v1/projects")
        .header(header::COOKIE, &session_token)
        .body(Some(&project_params))
        .expect_status(Some(StatusCode::UNAUTHORIZED))
        .execute()
        .await
        .expect("failed to get 401 for unauthed API request");

    RequestBuilder::new(&testctx, Method::GET, "/projects/whatever")
        .header(header::COOKIE, &session_token)
        .expect_status(Some(StatusCode::FOUND))
        .execute()
        .await
        .expect("failed to get 302 for unauthed console request");
}

async fn expect_console_page(
    testctx: &ClientTestContext,
    path: &str,
    session_token: Option<String>,
) {
    let mut builder = RequestBuilder::new(testctx, Method::GET, path);

    if let Some(session_token) = session_token {
        builder = builder.header(http::header::COOKIE, &session_token)
    }

    let console_page = builder
        .expect_console_asset()
        .expect_response_header(
            http::header::CONTENT_TYPE,
            "text/html; charset=utf-8",
        )
        .expect_response_header(http::header::CACHE_CONTROL, "no-store")
        .execute()
        .await
        .expect("failed to get console index");

    assert_eq!(console_page.body, "<html></html>".as_bytes());
}

#[nexus_test]
async fn test_console_pages(cptestctx: &ControlPlaneTestContext) {
    let testctx = &cptestctx.external_client;

    // request to console page route without auth should redirect to IdP
    expect_redirect(
        testctx,
        "/projects/irrelevant-path",
        &format!(
            "/login/{}/local?redirect_uri=%2Fprojects%2Firrelevant-path",
            cptestctx.silo_name
        ),
    )
    .await;

    let session_token = log_in_and_extract_token(cptestctx).await;

    // hit console pages with session, should get back HTML response
    let console_paths = &[
        "/",
        "/projects/irrelevant-path",
        "/projects-new",
        "/settings/irrelevant-path",
        "/system/irrelevant-path",
        "/device/success",
        "/device/verify",
        "/images",
        "/utilization",
        "/access",
        "/lookup/",
        "/lookup/abc",
    ];

    for path in console_paths {
        expect_console_page(testctx, path, Some(session_token.clone())).await;
    }
}

#[nexus_test]
async fn test_unauthed_console_pages(cptestctx: &ControlPlaneTestContext) {
    let testctx = &cptestctx.external_client;

    let unauthed_console_paths =
        &["/login/irrelevant-silo/local", "/login/irrelevant-silo/saml/my-idp"];

    for path in unauthed_console_paths {
        expect_console_page(testctx, path, None).await;
    }
}

#[nexus_test]
async fn test_assets(cptestctx: &ControlPlaneTestContext) {
    let testctx = &cptestctx.external_client;

    // nonexistent file 404s
    let _ =
        RequestBuilder::new(&testctx, Method::GET, "/assets/nonexistent.svg")
            .expect_status(Some(StatusCode::NOT_FOUND))
            .execute()
            .await
            .expect("failed to 404 on nonexistent asset");

    // existing file with disallowed extension 404s
    let _ = RequestBuilder::new(&testctx, Method::GET, "/assets/blocked.ext")
        .expect_status(Some(StatusCode::NOT_FOUND))
        .execute()
        .await
        .expect("failed to 404 on disallowed extension");

    // symlink 404s
    let _ = RequestBuilder::new(&testctx, Method::GET, "/assets/a_symlink")
        .expect_status(Some(StatusCode::NOT_FOUND))
        .execute()
        .await
        .expect("failed to 404 on symlink");

    // existing file is returned
    let resp = RequestBuilder::new(&testctx, Method::GET, "/assets/hello.txt")
        .expect_console_asset()
        .expect_response_header(
            http::header::CACHE_CONTROL,
            "max-age=31536000, immutable",
        )
        .expect_response_header(http::header::CONTENT_LENGTH, 11)
        .execute()
        .await
        .expect("failed to get existing file");

    assert_eq!(resp.body, "hello there".as_bytes());
    // make sure we're not including the gzip header on non-gzipped files
    assert_eq!(resp.headers.get(http::header::CONTENT_ENCODING), None);

    // file in a directory is returned
    let resp = RequestBuilder::new(
        &testctx,
        Method::GET,
        "/assets/a_directory/another_file.txt",
    )
    .expect_console_asset()
    .expect_response_header(
        http::header::CACHE_CONTROL,
        "max-age=31536000, immutable",
    )
    .expect_response_header(http::header::CONTENT_LENGTH, 10)
    .execute()
    .await
    .expect("failed to get existing file");

    assert_eq!(resp.body, "some words".as_bytes());
    // make sure we're not including the gzip header on non-gzipped files
    assert_eq!(resp.headers.get(http::header::CONTENT_ENCODING), None);

    // file with only gzipped version 404s if request doesn't have accept-encoding: gzip
    let _ = RequestBuilder::new(&testctx, Method::GET, "/assets/gzip-only.txt")
        .expect_status(Some(StatusCode::NOT_FOUND))
        .execute()
        .await
        .expect("failed to 404 on gzip file without accept-encoding: gzip");

    // file with only non-gzipped version is returned even if accept requests gzip
    let resp = RequestBuilder::new(&testctx, Method::GET, "/assets/hello.txt")
        .header(http::header::ACCEPT_ENCODING, "gzip")
        .expect_console_asset()
        .expect_response_header(
            http::header::CACHE_CONTROL,
            "max-age=31536000, immutable",
        )
        .expect_response_header(http::header::CONTENT_LENGTH, 11)
        .execute()
        .await
        .expect("failed to get existing file");

    assert_eq!(resp.body, "hello there".as_bytes());
    // make sure we're not including the gzip header on non-gzipped files
    assert_eq!(resp.headers.get(http::header::CONTENT_ENCODING), None);

    // file with only gzipped version is returned if request accepts gzip
    let resp =
        RequestBuilder::new(&testctx, Method::GET, "/assets/gzip-only.txt")
            .header(http::header::ACCEPT_ENCODING, "gzip")
            .expect_console_asset()
            .expect_response_header(http::header::CONTENT_ENCODING, "gzip")
            .expect_response_header(http::header::CONTENT_LENGTH, 16)
            .execute()
            .await
            .expect("failed to get existing file");

    assert_eq!(resp.body, "nothing but gzip".as_bytes());

    // file with both gzip and not returns gzipped if request accepts gzip
    let resp =
        RequestBuilder::new(&testctx, Method::GET, "/assets/gzip-and-not.txt")
            .header(http::header::ACCEPT_ENCODING, "gzip")
            .expect_console_asset()
            .expect_response_header(http::header::CONTENT_ENCODING, "gzip")
            .expect_response_header(
                http::header::CACHE_CONTROL,
                "max-age=31536000, immutable",
            )
            .expect_response_header(http::header::CONTENT_LENGTH, 33)
            .execute()
            .await
            .expect("failed to get existing file");

    assert_eq!(resp.body, "pretend this is gzipped beep boop".as_bytes());

    // returns non-gzipped if request doesn't accept gzip
    let resp =
        RequestBuilder::new(&testctx, Method::GET, "/assets/gzip-and-not.txt")
            .expect_console_asset()
            .expect_response_header(http::header::CONTENT_LENGTH, 28)
            .execute()
            .await
            .expect("failed to get existing file");

    assert_eq!(resp.body, "not gzipped but I know a guy".as_bytes());
    // make sure we're not including the gzip header on non-gzipped files
    assert_eq!(resp.headers.get(http::header::CONTENT_ENCODING), None);

    // test that `..` is not allowed in paths. (Dropshot handles this, so we
    // test to ensure this hasn't gone away.)
    let _ = RequestBuilder::new(
        &testctx,
        Method::GET,
        "/assets/../assets/hello.txt",
    )
    .expect_status(Some(StatusCode::BAD_REQUEST))
    .execute()
    .await
    .expect("failed to 400 on `..` traversal");
}

#[tokio::test]
async fn test_absolute_static_dir() {
    let mut config = load_test_config();
    config.pkg.console.static_dir =
        Utf8PathBuf::try_from(current_dir().unwrap())
            .unwrap()
            .join("tests/static");
    let cptestctx = test_setup_with_config::<omicron_nexus::Server>(
        "test_absolute_static_dir",
        &mut config,
        sim::SimMode::Explicit,
        None,
        0,
    )
    .await;
    let testctx = &cptestctx.external_client;

    // existing file is returned
    let resp = RequestBuilder::new(&testctx, Method::GET, "/assets/hello.txt")
        .expect_console_asset()
        .execute()
        .await
        .expect("failed to get existing file");

    assert_eq!(resp.body, "hello there".as_bytes());

    cptestctx.teardown().await;
}

#[nexus_test]
async fn test_session_me(cptestctx: &ControlPlaneTestContext) {
    let testctx = &cptestctx.external_client;

    // hitting /v1/me without being logged in is a 401
    RequestBuilder::new(&testctx, Method::GET, "/v1/me")
        .expect_status(Some(StatusCode::UNAUTHORIZED))
        .execute()
        .await
        .expect("failed to 401 on unauthed request");

    // now make same request with auth
    let priv_user = NexusRequest::object_get(testctx, "/v1/me")
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to get current user")
        .parsed_body::<views::CurrentUser>()
        .unwrap();

    assert_eq!(
        priv_user,
        views::CurrentUser {
            user: views::User {
                id: USER_TEST_PRIVILEGED.id(),
                display_name: USER_TEST_PRIVILEGED.external_id.clone(),
                silo_id: DEFAULT_SILO.id(),
            },
            silo_name: DEFAULT_SILO.name().clone()
        }
    );

    let unpriv_user = NexusRequest::object_get(testctx, "/v1/me")
        .authn_as(AuthnMode::UnprivilegedUser)
        .execute_and_parse_unwrap::<views::CurrentUser>()
        .await;

    assert_eq!(
        unpriv_user,
        views::CurrentUser {
            user: views::User {
                id: USER_TEST_UNPRIVILEGED.id(),
                display_name: USER_TEST_UNPRIVILEGED.external_id.clone(),
                silo_id: DEFAULT_SILO.id(),
            },
            silo_name: DEFAULT_SILO.name().clone()
        }
    );
}

#[nexus_test]
async fn test_session_me_groups(cptestctx: &ControlPlaneTestContext) {
    let testctx = &cptestctx.external_client;

    // hitting /v1/me without being logged in is a 401
    RequestBuilder::new(&testctx, Method::GET, "/v1/me/groups")
        .expect_status(Some(StatusCode::UNAUTHORIZED))
        .execute()
        .await
        .expect("failed to 401 on unauthed request");

    // That's true even if we use spoof authentication.
    let reqwest_builder = reqwest::ClientBuilder::new();
    let base_url = format!(
        "http://{}:{}",
        testctx.bind_address.ip(),
        testctx.bind_address.port()
    );
    assert!(
        omicron_test_utils::test_spoof_works(reqwest_builder, &base_url)
            .await
            .unwrap(),
        "unexpectedly failed to use spoof authn from test suite"
    );

    // now make same request with auth
    let priv_user_groups = NexusRequest::object_get(testctx, "/v1/me/groups")
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to get current user")
        .parsed_body::<ResultsPage<views::Group>>()
        .unwrap();

    assert_eq!(priv_user_groups.items, vec![]);

    let unpriv_user_groups = NexusRequest::object_get(testctx, "/v1/me/groups")
        .authn_as(AuthnMode::UnprivilegedUser)
        .execute()
        .await
        .expect("failed to get current user")
        .parsed_body::<ResultsPage<views::Group>>()
        .unwrap();

    assert_eq!(unpriv_user_groups.items, vec![]);
}

#[nexus_test]
async fn test_login_redirect_simple(cptestctx: &ControlPlaneTestContext) {
    let testctx = &cptestctx.external_client;

    let expected = format!("/login/{}/local", cptestctx.silo_name);
    expect_redirect(testctx, "/login", &expected).await;

    // pass through state param to login redirect URL. keep it URL encoded, don't double encode
    // encoded path is /abc/def
    expect_redirect(
        testctx,
        "/login?redirect_uri=%2Fabc%2Fdef",
        &format!("{}?redirect_uri=%2Fabc%2Fdef", expected),
    )
    .await;

    // if state param comes in not URL encoded, we should still URL encode it
    expect_redirect(
        testctx,
        "/login?redirect_uri=/abc/def",
        &format!("{}?redirect_uri=%2Fabc%2Fdef", expected),
    )
    .await;
}

// reject anything that's not a relative URI
#[nexus_test]
async fn test_bad_redirect_uri(cptestctx: &ControlPlaneTestContext) {
    let testctx = &cptestctx.external_client;

    let paths = [
        "/login",
        "/login/my-silo/local",
        "/login/my-silo/saml/my-idp",
        "/login/my-silo/saml/my-idp/redirect",
    ];
    let bad_uris = ["foo", "", "http://example.com"];

    for path in paths.iter() {
        for bad_uri in bad_uris.iter() {
            let uri = format!("{}?redirect_uri={}", path, bad_uri);
            let body = RequestBuilder::new(testctx, Method::GET, &uri)
                .expect_status(Some(StatusCode::BAD_REQUEST))
                .execute()
                .await
                .expect("failed to 400")
                .parsed_body::<dropshot::HttpErrorResponseBody>()
                .unwrap();
            assert!(body.message.ends_with("not a relative URI"));
        }
    }
}

#[nexus_test]
async fn test_login_redirect_multiple_silos(
    cptestctx: &ControlPlaneTestContext,
) {
    // With a fresh ControlPlaneTestContext, we have the built-in Silo (which
    // cannot be logged into) and a recovery Silo (a local-only Silo).  We'll
    // create a few other Silos:
    //
    // - saml-0-idps: a SAML Silo without any IdPs
    // - saml-1-idp: a SAML Silo with one IdP
    // - saml-2-idps: a SAML Silo with two IdPS
    //
    // Then we'll hit the endpoints for each of these Silos (including the
    // recovery Silo) and test the login redirect behavior.

    let client = &cptestctx.external_client;
    let silo_saml0 =
        create_silo(&client, "saml-0-idps", false, SiloIdentityMode::SamlJit)
            .await;
    let silo_saml1 =
        create_silo(&client, "saml-1-idp", false, SiloIdentityMode::SamlJit)
            .await;
    let silo_saml2 =
        create_silo(&client, "saml-2-idps", false, SiloIdentityMode::SamlJit)
            .await;

    for (i, silo) in [&silo_saml1, &silo_saml2].into_iter().enumerate() {
        let nidps = i + 1;
        for j in 0..nidps {
            let idp_name = format!("idp{}", j);
            let idp_params = params::SamlIdentityProviderCreate {
                identity: IdentityMetadataCreateParams {
                    name: idp_name.parse().unwrap(),
                    description: format!(
                        "silo {:?} idp {:?}",
                        &silo.identity.name, idp_name
                    ),
                },

                idp_metadata_source:
                    params::IdpMetadataSource::Base64EncodedXml {
                        data: base64::engine::general_purpose::STANDARD
                            .encode(SAML_RESPONSE_IDP_DESCRIPTOR),
                    },

                idp_entity_id: "https://some.idp.test/oxide_rack/".to_string(),
                sp_client_id: "client_id".to_string(),
                acs_url: "https://customer.test/oxide_rack/saml".to_string(),
                slo_url: "https://customer.test/oxide_rack/saml".to_string(),
                technical_contact_email: "technical@test.test".to_string(),

                signing_keypair: None,

                group_attribute_name: None,
            };
            let idp_create_url = format!(
                "/v1/system/identity-providers/saml?silo={}",
                &silo.identity.name
            );
            let _: views::SamlIdentityProvider =
                object_create(client, &idp_create_url, &idp_params).await;
        }
    }

    // For these tests, we need our reqwest client to act as though it resolved
    // Nexus's IPs via DNS.  We could use our own external DNS server, but that
    // won't always work because we want to exercise cases where the name isn't
    // in DNS (e.g., because the Silo has been deleted or because it's a made-up
    // name altogether).  So instead we just configure the reqwest client
    // directly with these DNS resolutions.
    let mut reqwest_builder = reqwest::ClientBuilder::new()
        .redirect(reqwest::redirect::Policy::none());
    let port = client.bind_address.port();
    for name in [
        "not-a-silo",
        cptestctx.silo_name.as_str(),
        silo_saml0.identity.name.as_str(),
        silo_saml1.identity.name.as_str(),
        silo_saml2.identity.name.as_str(),
    ] {
        let dns_name = format!("{}.sys.{}", name, DNS_ZONE_EXTERNAL_TESTING);
        // reqwest does not use this port.
        reqwest_builder = reqwest_builder
            .resolve(&dns_name, (client.bind_address.ip(), port).into());
    }

    let reqwest_client = reqwest_builder.build().unwrap();

    #[derive(Debug, PartialEq, Eq)]
    enum Redirect {
        Error(String),
        Location(String),
    }

    async fn make_request(
        client: &reqwest::Client,
        silo_name: &str,
        port: u16,
        state: Option<&str>,
    ) -> Redirect {
        let query =
            state.map_or("".to_string(), |s| format!("?redirect_uri={}", s));
        let url = format!(
            "http://{}.sys.{}:{}/login{}",
            silo_name, DNS_ZONE_EXTERNAL_TESTING, port, query
        );
        let response = client
            .get(&url)
            .send()
            .await
            .with_context(|| format!("GET {}", url))
            .unwrap();
        let status = response.status();
        if status == http::StatusCode::BAD_REQUEST {
            let error: dropshot::HttpErrorResponseBody = response
                .json()
                .await
                .with_context(|| {
                    format!("GET {:?}: failed to read 400 response body", url)
                })
                .unwrap();
            Redirect::Error(error.message)
        } else if status == http::StatusCode::FOUND {
            let location = response
                .headers()
                .get(http::header::LOCATION)
                .unwrap_or_else(|| {
                    panic!(
                        "GET {:?}: missing \"location\" header on 302 response",
                        url
                    )
                })
                .to_str()
                .unwrap_or_else(|e| {
                    panic!(
                        "GET {:?}: parsing \"location\" header on 302 \
                        response: {:#}",
                        url, e
                    );
                });
            Redirect::Location(String::from(location))
        } else {
            panic!("GET {:?}: unexpected response code", url);
        }
    }

    // Wait for Nexus to finish updating its external endpoint configuration.
    // This is asynchronous with respect to Silo creation.
    let _ = wait_for_condition(
        || async {
            let url = format!(
                "http://{}.sys.{}:{}/login",
                silo_saml2.identity.name, DNS_ZONE_EXTERNAL_TESTING, port
            );
            let response = match reqwest_client.get(&url).send().await {
                Ok(response) => response,
                Err(error) => {
                    eprintln!("wait for nexus update: {:?}", error);
                    if error.is_connect() {
                        // DNS may not have been updated yet.
                        return Err(CondCheckError::NotYet);
                    }

                    return Err(CondCheckError::Failed(error));
                }
            };

            eprintln!("wait for nexus update: status {:?}", response.status());
            if response.status() == http::StatusCode::BAD_REQUEST {
                // Nexus may not have updated its endpoint configuration yet.
                return Err(CondCheckError::NotYet);
            }

            // For any other response, we'll proceed.  It may be wrong, but the
            // subsequent checks will identify that with a clearer message than
            // we can do here.
            Ok(())
        },
        &std::time::Duration::from_millis(50),
        &std::time::Duration::from_secs(30),
    )
    .await
    .unwrap();

    // Recovery silo: redirect for local username/password login
    assert_eq!(
        make_request(&reqwest_client, cptestctx.silo_name.as_str(), port, None)
            .await,
        Redirect::Location(format!("/login/{}/local", &cptestctx.silo_name,)),
    );

    // same thing, but with state param in URL
    assert_eq!(
        make_request(
            &reqwest_client,
            cptestctx.silo_name.as_str(),
            port,
            Some("/abc/def")
        )
        .await,
        Redirect::Location(format!(
            "/login/{}/local?redirect_uri=%2Fabc%2Fdef",
            &cptestctx.silo_name,
        )),
    );

    // SAML with no idps: no redirect possible
    assert_eq!(
        make_request(
            &reqwest_client,
            silo_saml0.identity.name.as_str(),
            port,
            None
        )
        .await,
        Redirect::Error(String::from(
            "no identity providers are configured for Silo"
        ))
    );
    // SAML with one idp: redirect to that idp
    assert_eq!(
        make_request(
            &reqwest_client,
            silo_saml1.identity.name.as_str(),
            port,
            None
        )
        .await,
        Redirect::Location(format!(
            "/login/{}/saml/idp0",
            silo_saml1.identity.name.as_str()
        )),
    );
    // same thing but, with state param in URL
    assert_eq!(
        make_request(
            &reqwest_client,
            silo_saml1.identity.name.as_str(),
            port,
            Some("/abc/def"),
        )
        .await,
        Redirect::Location(format!(
            "/login/{}/saml/idp0?redirect_uri=%2Fabc%2Fdef",
            silo_saml1.identity.name.as_str()
        )),
    );
    // SAML with two idps: redirect to the first one
    // This is arbitrary.  We just don't want /login to break if you add a
    // second IdP.
    assert_eq!(
        make_request(
            &reqwest_client,
            silo_saml2.identity.name.as_str(),
            port,
            None
        )
        .await,
        Redirect::Location(format!(
            "/login/{}/saml/idp0",
            silo_saml2.identity.name.as_str()
        )),
    );

    // Bogus Silo: this currently redirects you to _some_ Silo.
    assert_matches::assert_matches!(
        make_request(&reqwest_client, "not-a-silo", port, None).await,
        Redirect::Location(_)
    );

    // Remove all of the Silos above.
    for silo_name in [
        &cptestctx.silo_name,
        &silo_saml0.identity.name,
        &silo_saml1.identity.name,
        &silo_saml2.identity.name,
    ] {
        let url = format!("/v1/system/silos/{}", silo_name);
        NexusRequest::object_delete(&client, &url)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .with_context(|| format!("DELETE {:?}", url))
            .unwrap();
    }

    // Try hitting one of the endpoints until we get an error.  This may take a
    // bit since Nexus updating its endpoint configuration is asynchronous.
    // (This is backwards from most uses of wait_for_condition because the
    // *success* case is an error.)
    let message = wait_for_condition::<_, (), _, _>(
        || async {
            match make_request(
                &reqwest_client,
                silo_saml1.identity.name.as_str(),
                port,
                None,
            )
            .await
            {
                Redirect::Location(_) => Err(CondCheckError::NotYet),
                Redirect::Error(message) => Ok(message),
            }
        },
        &std::time::Duration::from_millis(50),
        &std::time::Duration::from_secs(30),
    )
    .await
    .unwrap();
    assert_eq!(
        message,
        format!(
            "HTTP request for unknown server name \"{}.sys.{}\"",
            silo_saml1.identity.name, DNS_ZONE_EXTERNAL_TESTING,
        )
    )
}

fn get_header_value(resp: TestResponse, header_name: HeaderName) -> String {
    resp.headers.get(header_name).unwrap().to_str().unwrap().to_string()
}

async fn log_in_and_extract_token(
    cptestctx: &ControlPlaneTestContext,
) -> String {
    let testctx = &cptestctx.external_client;
    let url = format!("/v1/login/{}/local", cptestctx.silo_name);
    let credentials = test_params::UsernamePasswordCredentials {
        username: cptestctx.user_name.as_ref().parse().unwrap(),
        password: TEST_SUITE_PASSWORD.to_string(),
    };
    let login = RequestBuilder::new(&testctx, Method::POST, &url)
        .body(Some(&credentials))
        .expect_status(Some(StatusCode::NO_CONTENT))
        .execute()
        .await
        .expect("failed to log in");

    let session_cookie = get_header_value(login, header::SET_COOKIE);
    let (session_token, rest) = session_cookie.split_once("; ").unwrap();

    assert!(session_token.starts_with("session="));
    assert_eq!(rest, "Path=/; HttpOnly; SameSite=Lax; Max-Age=86400");

    session_token.to_string()
}

async fn expect_redirect(testctx: &ClientTestContext, from: &str, to: &str) {
    let _ = RequestBuilder::new(&testctx, Method::GET, from)
        .expect_status(Some(StatusCode::FOUND))
        .expect_response_header(header::LOCATION, to)
        .execute()
        .await
        .expect("did not find expected redirect");
}

/// Make sure an expired session gets deleted when you try to use it
///
/// This is not the best kind of test because it breaks the API abstraction
/// boundary, but in this case it's necessary because by design we do not
/// expose through the API why authn failed, i.e., whether it's because
/// the session was found but is expired. vs not found at all
#[tokio::test]
async fn test_session_idle_timeout_deletes_session() {
    // set idle timeout to 1 second so we can test expiration
    let mut config = load_test_config();
    config.pkg.console.session_idle_timeout_minutes = 0;
    let cptestctx = test_setup_with_config::<omicron_nexus::Server>(
        "test_session_idle_timeout_deletes_session",
        &mut config,
        sim::SimMode::Explicit,
        None,
        0,
    )
    .await;
    let testctx = &cptestctx.external_client;

    // Start session
    let session_cookie = log_in_and_extract_token(&cptestctx).await;

    // sleep here not necessary given TTL of 0

    // Make a request with the expired session cookie
    let me_response = RequestBuilder::new(testctx, Method::GET, "/v1/me")
        .header(header::COOKIE, &session_cookie)
        .expect_status(Some(StatusCode::UNAUTHORIZED))
        .execute()
        .await
        .expect("first request with expired cookie did not 401");

    let error_body: dropshot::HttpErrorResponseBody =
        me_response.parsed_body().unwrap();
    assert_eq!(error_body.message, "credentials missing or invalid");

    // check that the session actually got deleted

    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    let token = session_cookie.strip_prefix("session=").unwrap();
    let db_token_error = nexus
        .datastore()
        .session_lookup_by_token(&opctx, token.to_string())
        .await
        .expect_err("session should be deleted");
    assert_matches::assert_matches!(
        db_token_error,
        Error::ObjectNotFound { .. }
    );
}
