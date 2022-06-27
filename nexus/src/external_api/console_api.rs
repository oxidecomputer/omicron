// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Handler functions (entrypoints) for console-related routes.
//!
//! This was originally conceived as a separate dropshot server from the
//! external API, but in order to avoid CORS issues for now, we are serving
//! these routes directly from the external API.
use super::views;
use crate::authn::{
    silos::IdentityProviderType, USER_TEST_PRIVILEGED, USER_TEST_UNPRIVILEGED,
};
use crate::context::OpContext;
use crate::ServerContext;
use crate::{
    authn::external::{
        cookies::Cookies,
        session_cookie::{
            clear_session_cookie_header_value, session_cookie_header_value,
            SessionStore, SESSION_COOKIE_COOKIE_NAME,
        },
    },
    db::identity::Asset,
};
use anyhow::Context;
use dropshot::{
    endpoint, HttpError, HttpResponseOk, Path, Query, RequestContext, TypedBody,
};
use http::{header, Response, StatusCode};
use hyper::Body;
use lazy_static::lazy_static;
use mime_guess;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_urlencoded;
use std::{collections::HashSet, ffi::OsString, path::PathBuf, sync::Arc};
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SpoofLoginBody {
    pub username: String,
}

// This is just for demo purposes. we will probably end up with a real
// username/password login endpoint, but I think it will only be for use while
// setting up the rack
#[endpoint {
   method = POST,
   path = "/login",
   // TODO: this should be unpublished, but for now it's convenient for the
   // console to use the generated client for this request
   tags = ["hidden"],
}]
pub async fn spoof_login(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    params: TypedBody<SpoofLoginBody>,
) -> Result<Response<Body>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let params = params.into_inner();
    let user_id: Option<Uuid> = match params.username.as_str() {
        "privileged" => Some(USER_TEST_PRIVILEGED.id()),
        "unprivileged" => Some(USER_TEST_UNPRIVILEGED.id()),
        _ => None,
    };

    if user_id.is_none() {
        return Ok(Response::builder()
            .status(StatusCode::UNAUTHORIZED)
            .header(header::SET_COOKIE, clear_session_cookie_header_value())
            .body("unauthorized".into())?); // TODO: failed login response body?
    }

    let user_id = user_id.unwrap();

    // For now, we use the external authn context to create the session.
    // Once we have real SAML login, maybe we can cons up a real OpContext for
    // this user and use their own privileges to create the session.
    let authn_opctx = nexus.opctx_external_authn();
    let session = nexus.session_create(&authn_opctx, user_id).await?;

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(
            header::SET_COOKIE,
            session_cookie_header_value(
                &session.token,
                apictx.session_idle_timeout(),
            ),
        )
        .body("ok".into())?) // TODO: what do we return from login?
}

// Silos have one or more identity providers, and an unauthenticated user will
// be asked to authenticate to one of those below. Silo identity provider
// selection is currently performed as a name on the /login/ path. This will
// probably change in the future.
//
// Nexus currently supports using a SAML identity provider (IdP), and both login
// and logout flows are explained below:
//
// SAML login flow
// --------------
//
// (Note that https://duo.com/blog/the-beer-drinkers-guide-to-saml is a good
// reference)
//
// Nexus in this case is the service provider (SP), and when the silo identity
// provider is type SAML, SP initiated login flow will begin when the user does
// a GET /login/{silo_name}/{provider_name}.
//
// But before that, as an example, say an unauthenticated user (or a user whose
// credentials have expired) tries to navigate to:
//
//   GET /organizations/myorg
//
// If the user has expired credentials, their user id can be looked up from
// their session token, and perhaps even the last identity provider name they
// used may also be stored in their cookie. The appropriate login URL can be
// created.
//
// TODO If the user does not have this information it's unclear what should
// happen.  If they know the silo name they are trying to log into, they could
// `GET /silos/{silo_name}/identity_providers` in order to list available
// identity providers. If not, TODO.
//
// Once the appropriate login URL is created, the user's browser is redirected:
//
//   GET /login/{silo_name}/{provider_name}
//
// For identity provider type SAML, this will cause Nexus to send a AuthnRequest
// to the selected IdP in the SAMLRequest parameter. It will optionally be
// signed depending if a signing key pair was supplied in the SAML provider
// configuration. Nexus currently supports the Redirect binding, meaning the
// user's browser will be redirected to their IdP's SSO login url:
//
//   https://some.idp.test/auth/saml?SAMLRequest=...&RelayState=...
//
// If the request has a signature, the query above will also contain SigAlg and
// Signature params:
//
//   https://some.idp.test/auth/saml?SAMLRequest=...&RelayState=...&SigAlg=...&Signature=...
//
// SAMLRequest is base64 encoded zlib compressed XML, and RelayState can be
// anything - Nexus currently encodes the referer header so that when SAML login
// is successful the user can be sent back to where they were originally.
//
// The user will then authenticate with that IdP, and if successful will be
// redirected back to the SP (Nexus) with a POST:
//
//   POST /login/{silo_name}/{provider_name}
//
// The body of this POST will contain a URL encoded payload that includes the
// IdP's SAMLResponse plus optional relay state:
//
//   SAMLResponse=...&RelayState=...
//
// The RelayState will match what was sent as part of the initial redirect to
// the IdP. The SAMLResponse will contain (among other things) the IdP's
// assertion that this user has authenticated correctly, and provide information
// about that user. Note that there is no Signature on the whole POST body, just
// embedded in the SAMLResponse!
//
// The IdP's SAMLResponse will authenticate a subject, and from this external
// subject a silo user has to be created or retrieved (depending on the Silo's
// user provision type). After that, users will be redirected to the referer in
// the relay state, or to `/organizations`.
//
// SAML logout flow
// ----------------
//
// ** TODO SAML logout is currently unimplemented! **
//
// SAML logout is either SP initiated or IDP initiated.
//
// For SP inititated, a user will navigate to some yet-to-be-determined Nexus
// URL. Something like:
//
//   GET /logout/{silo_name}/{provider_name}
//
// Nexus will redirect them to their IdP in order to perform the logout:
//
//   https://some.idp.test/auth/saml?SAMLRequest=...&RelayState=...
//
// where a LogoutRequest will be sent in the SAMLRequest parameter. If
// successful, the IDP will redirect the user's browser back to:
//
//   POST /logout/{silo_name}/{provider_name}?SAMLRequest=...
//
// where there will be a LogoutRequest in the SAMLRequest parameter (where now
// the IDP is requesting logout in the SP).
//
// For IDP inititated, the IDP can spontaneously POST a LogoutRequest to
//
//   /logout/{silo_name}/{provider_name}

#[derive(Deserialize, JsonSchema)]
pub struct LoginToProviderPathParam {
    pub silo_name: crate::db::model::Name,
    pub provider_name: crate::db::model::Name,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct RelayState {
    pub referer: Option<String>,
}

impl RelayState {
    pub fn to_encoded(&self) -> Result<String, anyhow::Error> {
        Ok(base64::encode(
            serde_json::to_string(&self).context("encoding relay state")?,
        ))
    }

    pub fn from_encoded(encoded: String) -> Result<Self, anyhow::Error> {
        serde_json::from_str(
            &String::from_utf8(
                base64::decode(encoded)
                    .context("base64 decoding relay state")?,
            )
            .context("creating relay state string")?,
        )
        .context("json from relay state string")
    }
}

/// Ask the user to login to their identity provider
///
/// Either display a page asking a user for their credentials, or redirect them
/// to their identity provider.
#[endpoint {
   method = GET,
   path = "/login/{silo_name}/{provider_name}",
   tags = ["login"],
}]
pub async fn login(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<LoginToProviderPathParam>,
) -> Result<Response<Body>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let path_params = path_params.into_inner();
        let request = &rqctx.request.lock().await;

        // Use opctx_external_authn because this request will be
        // unauthenticated.
        let opctx = nexus.opctx_external_authn();

        let (.., identity_provider) = IdentityProviderType::lookup(
            &nexus.datastore(),
            &opctx,
            &path_params.silo_name,
            &path_params.provider_name,
        )
        .await?;

        match identity_provider {
            IdentityProviderType::Saml(saml_identity_provider) => {
                // Relay state is sent to the IDP, to be sent back to the SP after a
                // successful login.
                let relay_state: Option<String> = if let Some(value) =
                    request.headers().get(hyper::header::REFERER)
                {
                    let relay_state = RelayState {
                        referer: {
                            Some(
                                value
                                    .to_str()
                                    .map_err(|e| {
                                        HttpError::for_internal_error(format!(
                                            "referer header to_str failed! {}",
                                            e
                                        ))
                                    })?
                                    .to_string(),
                            )
                        },
                    };

                    Some(relay_state.to_encoded().map_err(|e| {
                        HttpError::for_internal_error(format!(
                            "encoding relay state failed: {}",
                            e
                        ))
                    })?)
                } else {
                    None
                };

                let sign_in_url =
                    saml_identity_provider.sign_in_url(relay_state).map_err(
                        |e| HttpError::for_internal_error(e.to_string()),
                    )?;

                Ok(Response::builder()
                    .status(StatusCode::FOUND)
                    .header(http::header::LOCATION, sign_in_url)
                    .body("".into())?)
            }
        }
    };
    // TODO this doesn't work because the response is Response<Body>
    //apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
    handler.await
}

/// Consume some sort of credentials, and authenticate a user.
///
/// Either receive a username and password, or some sort of identity provider
/// data (like a SAMLResponse). Use these to set the user's session cookie.
#[endpoint {
   method = POST,
   path = "/login/{silo_name}/{provider_name}",
   tags = ["login"],
}]
pub async fn consume_credentials(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<LoginToProviderPathParam>,
    body_bytes: dropshot::UntypedBody,
) -> Result<Response<Body>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let path_params = path_params.into_inner();

        // Use opctx_external_authn because this request will be
        // unauthenticated.
        let opctx = nexus.opctx_external_authn();

        let (authz_silo, db_silo, identity_provider) =
            IdentityProviderType::lookup(
                &nexus.datastore(),
                &opctx,
                &path_params.silo_name,
                &path_params.provider_name,
            )
            .await?;

        let (authenticated_subject, relay_state_string) =
            match identity_provider {
                IdentityProviderType::Saml(saml_identity_provider) => {
                    let body_bytes = body_bytes.as_str()?;
                    saml_identity_provider.authenticated_subject(
                        &body_bytes,
                        nexus.samael_max_issue_delay(),
                    )?
                }
            };

        let relay_state: Option<RelayState> =
            if let Some(value) = relay_state_string {
                Some(RelayState::from_encoded(value).map_err(|e| {
                    HttpError::for_internal_error(format!("{}", e))
                })?)
            } else {
                None
            };

        let user = nexus
            .silo_user_from_authenticated_subject(
                &opctx,
                &authz_silo,
                &db_silo,
                &authenticated_subject,
            )
            .await?;

        if user.is_none() {
            info!(&apictx.log, "user is none");
            return Ok(Response::builder()
                .status(StatusCode::UNAUTHORIZED)
                .header(header::SET_COOKIE, clear_session_cookie_header_value())
                .body("unauthorized".into())?); // TODO: failed login response body?
        }

        let user = user.unwrap();

        // always create a new console session if the user is POSTing here.
        let session = nexus.session_create(&opctx, user.id()).await?;

        let next_url = if let Some(relay_state) = &relay_state {
            if let Some(referer) = &relay_state.referer {
                referer.clone()
            } else {
                "/organizations".to_string()
            }
        } else {
            "/organizations".to_string()
        };

        debug!(
            &apictx.log,
            "successful login to silo {} using provider {}: authenticated subject {} = user id {}",
            path_params.silo_name,
            path_params.provider_name,
            authenticated_subject.external_id,
            user.id(),
        );

        Ok(Response::builder()
            .status(StatusCode::FOUND)
            .header(http::header::LOCATION, next_url)
            .header(
                header::SET_COOKIE,
                session_cookie_header_value(
                    &session.token,
                    apictx.session_idle_timeout(),
                ),
            )
            .body("".into())?) // TODO: what do we return from login?
    };
    // TODO this doesn't work because the response is Response<Body>
    //apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
    handler.await
}

// Log user out of web console by deleting session in both server and browser
#[endpoint {
   // important for security that this be a POST despite the empty req body
   method = POST,
   path = "/logout",
   // TODO: this should be unpublished, but for now it's convenient for the
   // console to use the generated client for this request
   tags = ["hidden"],
}]
pub async fn logout(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    cookies: Cookies,
) -> Result<Response<Body>, HttpError> {
    let nexus = &rqctx.context().nexus;
    let opctx = OpContext::for_external_api(&rqctx).await;
    let token = cookies.get(SESSION_COOKIE_COOKIE_NAME);

    if let Ok(opctx) = opctx {
        if let Some(token) = token {
            nexus.session_hard_delete(&opctx, token.value()).await?;
        }
    }

    // If user's session was already expired, they failed auth and their session
    // was automatically deleted by the auth scheme. If they have no session
    // (e.g., they cleared their cookies while sitting on the page) they will
    // also fail auth.

    // Even if the user failed auth, we don't want to send them back a 401 like
    // we would for a normal request. They are in fact logged out like they
    // intended, and we should send the standard success response.

    Ok(Response::builder()
        .status(StatusCode::NO_CONTENT)
        .header(header::SET_COOKIE, clear_session_cookie_header_value())
        .body("".into())?)
}

#[derive(Deserialize, JsonSchema)]
pub struct RestPathParam {
    path: Vec<String>,
}

// Serve the console bundle without an auth gate just for the login form. This
// is meant to stand in for the customers identity provider. Since this is a
// placeholder, it's easiest to build the form into the console bundle. If we
// really wanted a login form, we would probably make it a standalone page,
// otherwise the user is downloading a bunch of JS for nothing.
#[endpoint {
   method = GET,
   path = "/spoof_login",
   unpublished = true,
}]
pub async fn spoof_login_form(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
) -> Result<Response<Body>, HttpError> {
    serve_console_index(rqctx.context()).await
}

#[derive(Deserialize, JsonSchema)]
pub struct StateParam {
    state: Option<String>,
}

// this happens to be the same as StateParam, but it may include other things
// later
#[derive(Serialize)]
pub struct LoginUrlQuery {
    // TODO: give state param the correct name. In SAML it's called RelayState.
    // If/when we support auth protocols other than SAML, we will need to have
    // separate implementations here for each one
    state: Option<String>,
}

/// Generate URL to IdP login form. Optional `state` param is included in query
/// string if present, and will typically represent the URL to send the user
/// back to after successful login.
fn get_login_url(state: Option<String>) -> String {
    // assume state is not URL encoded, so no risk of double encoding (dropshot
    // decodes it on the way in)
    let query = match state {
        Some(state) if state.is_empty() => None,
        Some(state) => Some(
            serde_urlencoded::to_string(LoginUrlQuery { state: Some(state) })
                // unwrap is safe because query.state was just deserialized out
                // of a query param, so we know it's serializable
                .unwrap(),
        ),
        None => None,
    };
    // Once we have IdP integration, this will be a URL for the IdP login page.
    // For now we point to our own placeholder login page.
    let mut url = "/spoof_login".to_string();
    if let Some(query) = query {
        url.push('?');
        url.push_str(query.as_str());
    }
    url
}

/// Redirect to IdP login URL
// Currently hard-coded to redirect to our own fake login form.
#[endpoint {
   method = GET,
   path = "/login",
   unpublished = true,
}]
pub async fn login_redirect(
    _rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    query_params: Query<StateParam>,
) -> Result<Response<Body>, HttpError> {
    let query = query_params.into_inner();
    Ok(Response::builder()
        .status(StatusCode::FOUND)
        .header(http::header::LOCATION, get_login_url(query.state))
        .body("".into())?)
}

/// Fetch the user associated with the current session
#[endpoint {
   method = GET,
   path = "/session/me",
   tags = ["hidden"],
}]
pub async fn session_me(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
) -> Result<HttpResponseOk<views::SessionUser>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        // TODO: we don't care about authentication method, as long as they are
        // authed as _somebody_. We could restrict this to session auth only,
        // but it's not clear what the advantage would be.
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let &actor = opctx.authn.actor_required()?;
        Ok(HttpResponseOk(actor.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// Dropshot does not have route match ranking and does not allow overlapping
// route definitions, so we cannot have a catchall `/*` route for console pages
// and then also define, e.g., `/api/blah/blah` and give the latter priority
// because it's a more specific match. So for now we simply give the console
// catchall route a prefix to avoid overlap. Long-term, if a route prefix is
// part of the solution, we would probably prefer it to be on the API endpoints,
// not on the console pages. Conveniently, all the console page routes start
// with /orgs already.
#[endpoint {
   method = GET,
   path = "/orgs/{path:.*}",
   unpublished = true,
}]
pub async fn console_page(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    _path_params: Path<RestPathParam>,
) -> Result<Response<Body>, HttpError> {
    let opctx = OpContext::for_external_api(&rqctx).await;

    // if authed, serve HTML page with bundle in script tag

    // HTML doesn't need to be static -- we'll probably find a reason to do some
    // minimal templating, e.g., putting a CSRF token in the page

    // amusingly, at least to start out, I don't think we care about the path
    // because the real routing is all client-side. we serve the same HTML
    // regardless, the app starts on the client and renders the right page and
    // makes the right API requests.
    if let Ok(opctx) = opctx {
        if opctx.authn.actor().is_some() {
            return serve_console_index(rqctx.context()).await;
        }
    }

    // otherwise redirect to idp
    Ok(Response::builder()
        .status(StatusCode::FOUND)
        .header(http::header::LOCATION, get_login_url(None))
        .body("".into())?)
}

/// Fetch a static asset from `<static_dir>/assets`. 404 on virtually all
/// errors. No auth. NO SENSITIVE FILES.
#[endpoint {
   method = GET,
   path = "/assets/{path:.*}",
   unpublished = true,
}]
pub async fn asset(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<RestPathParam>,
) -> Result<Response<Body>, HttpError> {
    let apictx = rqctx.context();
    let path = path_params.into_inner().path;

    let file = match &apictx.console_config.static_dir {
        // important: we only serve assets from assets/ within static_dir
        Some(static_dir) => find_file(path, &static_dir.join("assets")),
        _ => Err(not_found("static_dir undefined")),
    }?;
    let file_contents = tokio::fs::read(&file)
        .await
        .map_err(|e| not_found(&format!("accessing {:?}: {:#}", file, e)))?;

    // Derive the MIME type from the file name
    let content_type = mime_guess::from_path(&file)
        .first()
        .map_or_else(|| "text/plain".to_string(), |m| m.to_string());

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, &content_type)
        .header(http::header::CACHE_CONTROL, cache_control_header_value(apictx))
        .body(file_contents.into())?)
}

fn cache_control_header_value(apictx: &Arc<ServerContext>) -> String {
    format!(
        "max-age={}",
        apictx.console_config.cache_control_max_age.num_seconds()
    )
}

async fn serve_console_index(
    apictx: &Arc<ServerContext>,
) -> Result<Response<Body>, HttpError> {
    let static_dir = &apictx
        .console_config
        .static_dir
        .to_owned()
        .ok_or_else(|| not_found("static_dir undefined"))?;
    let file = static_dir.join(PathBuf::from("index.html"));
    let file_contents = tokio::fs::read(&file)
        .await
        .map_err(|e| not_found(&format!("accessing {:?}: {:#}", file, e)))?;
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, "text/html; charset=UTF-8")
        .header(http::header::CACHE_CONTROL, cache_control_header_value(apictx))
        .body(file_contents.into())?)
}

fn not_found(internal_msg: &str) -> HttpError {
    HttpError::for_not_found(None, internal_msg.to_string())
}

lazy_static! {
    static ref ALLOWED_EXTENSIONS: HashSet<OsString> = HashSet::from(
        [
            "js", "css", "html", "ico", "map", "otf", "png", "svg", "ttf",
            "txt", "woff", "woff2",
        ]
        .map(|s| OsString::from(s))
    );
}

fn file_ext_allowed(path: &PathBuf) -> bool {
    let ext = path
        .extension()
        .map(|ext| ext.to_os_string())
        .unwrap_or_else(|| OsString::from("disallowed"));
    ALLOWED_EXTENSIONS.contains(&ext)
}

/// Starting from `root_dir`, follow the segments of `path` down the file tree
/// until we find a file (or not). Do not follow symlinks.
fn find_file(
    path: Vec<String>,
    root_dir: &PathBuf,
) -> Result<PathBuf, HttpError> {
    let mut current = root_dir.to_owned(); // start from `root_dir`
    for segment in &path {
        // If we hit a non-directory thing already and we still have segments
        // left in the path, bail. We have nowhere to go.
        if !current.is_dir() {
            return Err(not_found("expected a directory"));
        }

        current.push(segment);

        // Don't follow symlinks.
        // Error means either the user doesn't have permission to pull
        // metadata or the path doesn't exist.
        let m = current
            .symlink_metadata()
            .map_err(|_| not_found("failed to get file metadata"))?;
        if m.file_type().is_symlink() {
            return Err(not_found("attempted to follow a symlink"));
        }
    }

    // can't serve a directory
    if current.is_dir() {
        return Err(not_found("expected a non-directory"));
    }

    if !file_ext_allowed(&current) {
        return Err(not_found("file extension not allowed"));
    }

    Ok(current)
}

#[cfg(test)]
mod test {
    use super::find_file;
    use http::StatusCode;
    use std::{env::current_dir, path::PathBuf};

    fn get_path(path_str: &str) -> Vec<String> {
        path_str.split("/").map(|s| s.to_string()).collect()
    }

    #[test]
    fn test_find_file_finds_file() {
        let root = current_dir().unwrap();
        let file = find_file(get_path("tests/static/assets/hello.txt"), &root);
        assert!(file.is_ok());
        let file = find_file(get_path("tests/static/index.html"), &root);
        assert!(file.is_ok());
    }

    #[test]
    fn test_find_file_404_on_nonexistent() {
        let root = current_dir().unwrap();
        let error = find_file(get_path("tests/static/nonexistent.svg"), &root)
            .unwrap_err();
        assert_eq!(error.status_code, StatusCode::NOT_FOUND);
        assert_eq!(error.internal_message, "failed to get file metadata",);
    }

    #[test]
    fn test_find_file_404_on_nonexistent_nested() {
        let root = current_dir().unwrap();
        let error =
            find_file(get_path("tests/static/a/b/c/nonexistent.svg"), &root)
                .unwrap_err();
        assert_eq!(error.status_code, StatusCode::NOT_FOUND);
        assert_eq!(error.internal_message, "failed to get file metadata")
    }

    #[test]
    fn test_find_file_404_on_directory() {
        let root = current_dir().unwrap();
        let error =
            find_file(get_path("tests/static/assets/a_directory"), &root)
                .unwrap_err();
        assert_eq!(error.status_code, StatusCode::NOT_FOUND);
        assert_eq!(error.internal_message, "expected a non-directory");
    }

    #[test]
    fn test_find_file_404_on_symlink() {
        let root = current_dir().unwrap();
        let path_str = "tests/static/assets/a_symlink";

        // the file in question does exist and is a symlink
        assert!(root
            .join(PathBuf::from(path_str))
            .symlink_metadata()
            .unwrap()
            .file_type()
            .is_symlink());

        // so we 404
        let error = find_file(get_path(path_str), &root).unwrap_err();
        assert_eq!(error.status_code, StatusCode::NOT_FOUND);
        assert_eq!(error.internal_message, "attempted to follow a symlink");
    }

    #[test]
    fn test_find_file_wont_follow_symlink() {
        let root = current_dir().unwrap();
        let path_str = "tests/static/assets/a_symlink/another_file.txt";

        // the file in question does exist
        assert!(root.join(PathBuf::from(path_str)).exists());

        // but it 404s because the path goes through a symlink
        let error = find_file(get_path(path_str), &root).unwrap_err();
        assert_eq!(error.status_code, StatusCode::NOT_FOUND);
        assert_eq!(error.internal_message, "attempted to follow a symlink");
    }

    #[test]
    fn test_find_file_404_on_disallowed_ext() {
        let root = current_dir().unwrap();
        let error =
            find_file(get_path("tests/static/assets/blocked.ext"), &root)
                .unwrap_err();
        assert_eq!(error.status_code, StatusCode::NOT_FOUND);
        assert_eq!(error.internal_message, "file extension not allowed",);

        let error = find_file(get_path("tests/static/assets/no_ext"), &root)
            .unwrap_err();
        assert_eq!(error.status_code, StatusCode::NOT_FOUND);
        assert_eq!(error.internal_message, "file extension not allowed");
    }
}
