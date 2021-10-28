//! Basic end-to-end tests of the authn facilities
// TODO-coverage We ought to add test cases that attempt to send requests with
// enormous header values or values that aren't allowed by HTTP.  This requires
// a lower-level interface, since our hyper Client will not allow us to send
// such invalid requests.

pub mod common;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use dropshot::endpoint;
use dropshot::test_util::read_json;
use dropshot::test_util::LogContext;
use dropshot::test_util::TestContext;
use dropshot::ApiDescription;
use dropshot::HttpErrorResponseBody;
use http::header::HeaderValue;
use omicron_nexus::authn::external::session_cookie;
use omicron_nexus::authn::external::spoof::HttpAuthnSpoof;
use omicron_nexus::authn::external::spoof::HTTP_HEADER_OXIDE_AUTHN_SPOOF;
use omicron_nexus::authn::external::spoof::SPOOF_RESERVED_BAD_ACTOR;
use omicron_nexus::authn::external::spoof::SPOOF_RESERVED_BAD_CREDS;
use omicron_nexus::authn::external::spoof::SPOOF_SCHEME_NAME;
use omicron_nexus::authn::external::HttpAuthnScheme;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

#[macro_use]
extern crate slog;

/// Tests authn::external::Authenticator with the "spoof" scheme allowed
///
/// This does not use Nexus or any of the rest of Omicron.  It sets up its own
/// Dropshot server with an endpoint that uses the authn facilities the same way
/// that Nexus might.
#[tokio::test]
async fn test_authn_spoof_allowed() {
    let test_name = "test_authn_spoof_allowed";
    let authn_schemes_configured: Vec<
        Box<dyn HttpAuthnScheme<WhoamiServerState> + 'static>,
    > = vec![Box::new(HttpAuthnSpoof)];
    let testctx = start_whoami_server(
        test_name,
        authn_schemes_configured,
        HashMap::new(),
    )
    .await;
    let tried_spoof = vec![SPOOF_SCHEME_NAME]
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<String>>();

    // Typical unauthenticated request
    assert_eq!(
        whoami_request(None, None, &testctx).await.unwrap(),
        WhoamiResponse {
            authenticated: false,
            actor: None,
            schemes_tried: tried_spoof.clone(),
        }
    );

    // Successful authentication
    let valid_uuid = "7f927c86-3371-4295-c34a-e3246a4b9c02";
    let header = HeaderValue::from_static(valid_uuid);
    assert_eq!(
        whoami_request(Some(header), None, &testctx).await.unwrap(),
        WhoamiResponse {
            authenticated: true,
            actor: Some(valid_uuid.to_owned()),
            schemes_tried: tried_spoof.clone(),
        }
    );

    // Bad header value (malformed)
    let header = HeaderValue::from_static("not-a-uuid");
    let (status_code, error) =
        whoami_request(Some(header), None, &testctx).await.unwrap_err();
    assert_eq!(error.error_code, None);
    assert!(error.message.starts_with(
        "bad authentication credentials: parsing header value as UUID"
    ));
    assert_eq!(status_code, http::StatusCode::BAD_REQUEST);

    // Unknown actor
    let header = HeaderValue::from_static(SPOOF_RESERVED_BAD_ACTOR);
    let (status_code, error) =
        whoami_request(Some(header), None, &testctx).await.unwrap_err();
    assert_authn_failed(status_code, &error);

    // Bad credentials
    let header = HeaderValue::from_static(SPOOF_RESERVED_BAD_CREDS);
    let (status_code, error) =
        whoami_request(Some(header), None, &testctx).await.unwrap_err();
    assert_authn_failed(status_code, &error);

    testctx.teardown().await;
}

#[tokio::test]
async fn test_authn_session_cookie() {
    let test_name = "test_authn_session_cookie";
    let authn_schemes_configured: Vec<
        Box<dyn HttpAuthnScheme<WhoamiServerState> + 'static>,
    > = vec![Box::new(session_cookie::HttpAuthnSessionCookie)];
    let valid_session = FakeSession {
        user_id: Uuid::new_v4(),
        time_last_used: Utc::now() - Duration::seconds(5),
        time_created: Utc::now() - Duration::seconds(5),
    };
    let idle_expired_session = FakeSession {
        user_id: Uuid::new_v4(),
        time_last_used: Utc::now() - Duration::hours(2),
        time_created: Utc::now() - Duration::hours(3),
    };
    let abs_expired_session = FakeSession {
        user_id: Uuid::new_v4(),
        time_last_used: Utc::now(),
        time_created: Utc::now() - Duration::hours(10),
    };
    let testctx = start_whoami_server(
        test_name,
        authn_schemes_configured,
        HashMap::from([
            ("valid".to_string(), valid_session),
            ("idle_expired".to_string(), idle_expired_session),
            ("abs_expired".to_string(), abs_expired_session),
        ]),
    )
    .await;

    let tried_cookie =
        vec![session_cookie::SESSION_COOKIE_SCHEME_NAME.to_string()];

    // with valid header
    let valid_header = HeaderValue::from_static("session=valid");
    assert_eq!(
        whoami_request(None, Some(valid_header), &testctx).await.unwrap(),
        WhoamiResponse {
            authenticated: true,
            actor: Some(valid_session.user_id.to_string()),
            schemes_tried: tried_cookie.clone(),
        }
    );

    // with idle expired session token
    let idle_expired_header = HeaderValue::from_static("session=idle_expired");
    let (status_code, error) =
        whoami_request(None, Some(idle_expired_header), &testctx)
            .await
            .unwrap_err();
    assert_authn_failed(status_code, &error);

    // with absolute expired session token
    let abs_expired_header = HeaderValue::from_static("session=abs_expired");
    let (status_code, error) =
        whoami_request(None, Some(abs_expired_header), &testctx)
            .await
            .unwrap_err();
    assert_authn_failed(status_code, &error);

    // with nonexistent session token
    let bad_header = HeaderValue::from_static("session=nonexistent");
    let (status_code, error) =
        whoami_request(None, Some(bad_header), &testctx).await.unwrap_err();
    assert_authn_failed(status_code, &error);

    // with no cookie
    let tried_cookie =
        vec![session_cookie::SESSION_COOKIE_SCHEME_NAME.to_string()];
    assert_eq!(
        whoami_request(None, None, &testctx).await.unwrap(),
        WhoamiResponse {
            authenticated: false,
            actor: None,
            schemes_tried: tried_cookie.clone(),
        }
    );

    testctx.teardown().await;
}

/// Like test_authn_spoof_allowed(), but tests against a server which has no
/// schemes configured.
#[tokio::test]
async fn test_authn_spoof_unconfigured() {
    let test_name = "test_authn_spoof_disallowed";
    let testctx =
        start_whoami_server(test_name, Vec::new(), HashMap::new()).await;

    let values = [
        None,
        Some(HeaderValue::from_static("7f927c86-3371-4295-c34a-e3246a4b9c02")),
        Some(HeaderValue::from_static("not-a-uuid")),
        Some(HeaderValue::from_static(SPOOF_RESERVED_BAD_ACTOR)),
        Some(HeaderValue::from_static(SPOOF_RESERVED_BAD_CREDS)),
    ];

    for v in values {
        assert_eq!(
            whoami_request(v, None, &testctx).await.unwrap(),
            WhoamiResponse {
                authenticated: false,
                actor: None,
                schemes_tried: Vec::new(),
            }
        );
    }

    testctx.teardown().await;
}

async fn whoami_request(
    spoof_header: Option<hyper::header::HeaderValue>,
    cookie_header: Option<hyper::header::HeaderValue>,
    testctx: &TestContext<WhoamiServerState>,
) -> Result<WhoamiResponse, (http::StatusCode, HttpErrorResponseBody)> {
    let client_testctx = &testctx.client_testctx;
    let mut builder = hyper::Request::builder()
        .method(http::method::Method::GET)
        .uri(client_testctx.url("/whoami"));

    if let Some(spoof_header_value) = spoof_header {
        builder =
            builder.header(HTTP_HEADER_OXIDE_AUTHN_SPOOF, spoof_header_value);
    }

    if let Some(cookie_header_value) = cookie_header {
        builder = builder.header(http::header::COOKIE, cookie_header_value);
    }

    let request = builder
        .body(hyper::Body::empty())
        .expect("attempted to construct invalid request");

    let mut response = hyper::Client::new()
        .request(request)
        .await
        .expect("failed to make request");
    if response.status() == http::StatusCode::OK {
        let whoami: WhoamiResponse = read_json(&mut response).await;
        info!(&testctx.log, "whoami response"; "whoami" => ?whoami);
        Ok(whoami)
    } else {
        let error_body: HttpErrorResponseBody = read_json(&mut response).await;
        info!(&testctx.log, "whoami error"; "error" => ?error_body);
        Err((response.status(), error_body))
    }
}

fn assert_authn_failed(
    status_code: http::StatusCode,
    error: &HttpErrorResponseBody,
) {
    assert_eq!(error.error_code, None);
    // Be very careful in changing this message or weakening this check.  It's
    // very intentional that we do not leak information about why authentication
    // failed.
    assert_eq!(error.message, "authentication failed");
    assert_eq!(status_code, http::StatusCode::UNAUTHORIZED);
}

async fn start_whoami_server(
    test_name: &str,
    authn_schemes_configured: Vec<Box<dyn HttpAuthnScheme<WhoamiServerState>>>,
    sessions: HashMap<String, FakeSession>,
) -> TestContext<WhoamiServerState> {
    let config = common::load_test_config();
    let logctx = LogContext::new(test_name, &config.log);

    let whoami_api = {
        let mut whoami_api = ApiDescription::new();
        whoami_api.register(whoami_get).unwrap_or_else(|error| {
            panic!("failed to register whoami_get: {:#}", error)
        });
        whoami_api
    };

    let server_state = {
        let authn = omicron_nexus::authn::external::Authenticator::new(
            authn_schemes_configured,
        );

        WhoamiServerState { authn, sessions: Mutex::new(sessions) }
    };

    let log = logctx.log.new(o!());
    TestContext::new(
        whoami_api,
        server_state,
        &config.dropshot_external,
        Some(logctx),
        log,
    )
}

struct WhoamiServerState {
    authn: omicron_nexus::authn::external::Authenticator<WhoamiServerState>,
    sessions: Mutex<HashMap<String, FakeSession>>,
}

#[derive(Clone, Copy)]
struct FakeSession {
    user_id: Uuid,
    time_created: DateTime<Utc>,
    time_last_used: DateTime<Utc>,
}

impl session_cookie::Session for FakeSession {
    fn user_id(&self) -> Uuid {
        self.user_id
    }
    fn time_created(&self) -> DateTime<Utc> {
        self.time_created
    }
    fn time_last_used(&self) -> DateTime<Utc> {
        self.time_last_used
    }
}

#[async_trait]
impl session_cookie::SessionStore for WhoamiServerState {
    type SessionModel = FakeSession;

    async fn session_fetch(&self, token: String) -> Option<Self::SessionModel> {
        self.sessions.lock().unwrap().get(&token).map(|s| *s)
    }

    async fn session_update_last_used(
        &self,
        token: String,
    ) -> Option<Self::SessionModel> {
        let mut sessions = self.sessions.lock().unwrap();
        let session = *sessions.get(&token).unwrap();
        let new_session = FakeSession { time_last_used: Utc::now(), ..session };
        (*sessions).insert(token, new_session)
    }

    async fn session_expire(&self, token: String) -> Option<()> {
        let mut sessions = self.sessions.lock().unwrap();
        (*sessions).remove(&token);
        Some(())
    }

    fn session_idle_timeout(&self) -> Duration {
        Duration::hours(1)
    }

    fn session_absolute_timeout(&self) -> Duration {
        Duration::hours(8)
    }
}

#[derive(
    Debug,
    serde::Deserialize,
    Eq,
    PartialEq,
    serde::Serialize,
    schemars::JsonSchema,
)]
struct WhoamiResponse {
    pub authenticated: bool,
    pub actor: Option<String>,
    pub schemes_tried: Vec<String>,
}

#[endpoint {
    method = GET,
    path = "/whoami",
}]
async fn whoami_get(
    rqctx: Arc<dropshot::RequestContext<WhoamiServerState>>,
) -> Result<dropshot::HttpResponseOk<WhoamiResponse>, dropshot::HttpError> {
    let whoami_state = rqctx.context();
    let authn = whoami_state.authn.authn_request(&rqctx).await?;
    let actor = authn.actor().map(|a| a.0.to_string());
    let authenticated = actor.is_some();
    let schemes_tried =
        authn.schemes_tried().iter().map(|s| s.to_string()).collect();
    Ok(dropshot::HttpResponseOk(WhoamiResponse {
        authenticated,
        actor,
        schemes_tried,
    }))
}
