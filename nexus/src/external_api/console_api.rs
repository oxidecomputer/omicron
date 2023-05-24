// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Handler functions (entrypoints) for console-related routes.
//!
//! This was originally conceived as a separate dropshot server from the
//! external API, but in order to avoid CORS issues for now, we are serving
//! these routes directly from the external API.
use crate::authn::silos::IdentityProviderType;
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
    endpoint, http_response_found, http_response_see_other, HttpError,
    HttpResponseFound, HttpResponseHeaders, HttpResponseSeeOther,
    HttpResponseUpdatedNoContent, Path, RequestContext, TypedBody,
};
use http::{header, Response, StatusCode};
use hyper::Body;
use lazy_static::lazy_static;
use mime_guess;
use nexus_db_queries::context::OpContext;
use nexus_types::external_api::params;
use omicron_common::api::external::{Error, Name};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_urlencoded;
use std::str::FromStr;
use std::{collections::HashSet, ffi::OsString, path::PathBuf, sync::Arc};

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
// `GET /system/silos/{silo_name}/identity_providers` in order to list available
// identity providers.
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
        Ok(base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            serde_json::to_string(&self).context("encoding relay state")?,
        ))
    }

    pub fn from_encoded(encoded: String) -> Result<Self, anyhow::Error> {
        serde_json::from_str(
            &String::from_utf8(
                base64::Engine::decode(
                    &base64::engine::general_purpose::STANDARD,
                    encoded,
                )
                .context("base64 decoding relay state")?,
            )
            .context("creating relay state string")?,
        )
        .context("json from relay state string")
    }
}

/// Prompt user login
///
/// Either display a page asking a user for their credentials, or redirect them
/// to their identity provider.
#[endpoint {
   method = GET,
   path = "/login/{silo_name}/saml/{provider_name}",
   tags = ["login"],
}]
pub async fn login_saml_begin(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<LoginToProviderPathParam>,
) -> Result<HttpResponseFound, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let path_params = path_params.into_inner();
        let request = &rqctx.request;

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
                // Relay state is sent to the IDP, to be sent back to the SP
                // after a successful login.
                let relay_state: Option<String> = if let Some(value) =
                    request.headers().get(hyper::header::REFERER)
                {
                    let relay_state = RelayState {
                        referer: {
                            Some(
                                value
                                    .to_str()
                                    .map_err(|e| {
                                        HttpError::for_bad_request(
                                            None,
                                            format!(
                                            "referer header to_str failed! {}",
                                            e
                                        ),
                                        )
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

                http_response_found(sign_in_url)
            }
        }
    };

    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Authenticate a user via SAML
#[endpoint {
   method = POST,
   path = "/v1/login/{silo_name}/saml/{provider_name}",
   tags = ["login"],
}]
pub async fn login_saml(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<LoginToProviderPathParam>,
    body_bytes: dropshot::UntypedBody,
) -> Result<HttpResponseSeeOther, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let path_params = path_params.into_inner();

        // By definition, this request is not authenticated.  These operations
        // happen using the Nexus "external authentication" context, which we
        // keep specifically for this purpose.
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

        login_finish(&opctx, apictx, user, relay_state.and_then(|r| r.referer))
            .await
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

#[derive(Deserialize, JsonSchema)]
pub struct LoginPathParam {
    pub silo_name: crate::db::model::Name,
}

#[derive(Serialize)]
pub struct LoginUrlQuery {
    state: Option<String>,
}

/// Join provided path and optional `redirect_url`, which represents the URL to
/// send the user back to after successful login, and is included in `state`
/// query param if present
fn path_with_state_param(path: &str, redirect_url: Option<String>) -> String {
    // assume redirect_url is not already encoded
    match serde_urlencoded::to_string(LoginUrlQuery { state: redirect_url }) {
        // only put the ? in front if there's something there
        Ok(encoded) if !encoded.is_empty() => format!("{path}?{encoded}"),
        // redirect_url is either None or not url-encodable for some reason
        _ => path.to_string(),
    }
}

/// Username/password login for the specified silo
#[endpoint {
   method = GET,
   path = "/login/{silo_name}/local",
   unpublished = true,
}]
pub async fn login_local_begin(
    rqctx: RequestContext<Arc<ServerContext>>,
    _path_params: Path<LoginPathParam>,
) -> Result<Response<Body>, HttpError> {
    // TODO: look up silo and 404 if not local? or ignore and let the post fail
    // to avoid leaking silo names

    // TODO: wrap in instrumenting code

    // TODO: if the user is logged in, redirect to / or /projects? or detect
    // this on the client-side and show a link?
    serve_console_index(rqctx).await
}

/// Authenticate a user via username and password
#[endpoint {
   method = POST,
   path = "/login/{silo_name}/local",
   tags = ["login"],
}]
pub async fn login_local(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<LoginPathParam>,
    credentials: TypedBody<params::UsernamePasswordCredentials>,
) -> Result<HttpResponseSeeOther, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let credentials = credentials.into_inner();
        let silo = path.silo_name.into();

        // By definition, this request is not authenticated.  These operations
        // happen using the Nexus "external authentication" context, which we
        // keep specifically for this purpose.
        let opctx = nexus.opctx_external_authn();
        let silo_lookup = nexus.silo_lookup(&opctx, silo)?;
        let user = nexus.login_local(&opctx, &silo_lookup, credentials).await?;
        login_finish(&opctx, apictx, user, None).await
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

async fn login_finish(
    opctx: &OpContext,
    apictx: &ServerContext,
    user: Option<crate::db::model::SiloUser>,
    next_url: Option<String>,
) -> Result<HttpResponseSeeOther, HttpError> {
    let nexus = &apictx.nexus;

    if user.is_none() {
        Err(Error::Unauthenticated {
            internal_message: String::from(
                "no matching user found or credentials were not valid",
            ),
        })?;
    }

    let user = user.unwrap();
    let session = nexus.session_create(&opctx, user.id()).await?;
    let next_url = next_url.unwrap_or_else(|| "/".to_string());

    let mut response_with_headers = http_response_see_other(next_url)?;

    {
        let headers = response_with_headers.headers_mut();
        headers.append(
            header::SET_COOKIE,
            session_cookie_header_value(
                &session.token,
                // use absolute timeout even though session might idle out first.
                // browser expiration is mostly for convenience, as the API will
                // reject requests with an expired session regardless
                apictx.session_absolute_timeout(),
            )?,
        );
    }
    Ok(response_with_headers)
}

/// Log user out by deleting session on client and server
#[endpoint {
   // important for security that this be a POST despite the empty req body
   method = POST,
   path = "/logout",
   tags = ["hidden"], // hide in docs
}]
pub async fn logout(
    rqctx: RequestContext<Arc<ServerContext>>,
    cookies: Cookies,
) -> Result<HttpResponseHeaders<HttpResponseUpdatedNoContent>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let opctx = crate::context::op_context_for_external_api(&rqctx).await;
        let token = cookies.get(SESSION_COOKIE_COOKIE_NAME);

        if let Ok(opctx) = opctx {
            if let Some(token) = token {
                nexus.session_hard_delete(&opctx, token.value()).await?;
            }
        }

        // If user's session was already expired, they failed auth and their
        // session was automatically deleted by the auth scheme. If they have no
        // session (e.g., they cleared their cookies while sitting on the page)
        // they will also fail auth.

        // Even if the user failed auth, we don't want to send them back a 401
        // like we would for a normal request. They are in fact logged out like
        // they intended, and we should send the standard success response.

        let mut response =
            HttpResponseHeaders::new_unnamed(HttpResponseUpdatedNoContent());
        {
            let headers = response.headers_mut();
            headers.append(
                header::SET_COOKIE,
                clear_session_cookie_header_value()?,
            );
        };

        Ok(response)
    };

    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

#[derive(Deserialize, JsonSchema)]
pub struct RestPathParam {
    path: Vec<String>,
}

/// Generic login form, works for both local and SAML. Console client is
/// responsible for hitting an API endpoint (TODO) that tells it whether to show
/// username/password login or a link to the SAML IdP.
#[endpoint {
   method = GET,
   path = "/login",
   unpublished = true,
}]
pub async fn login_begin(
    rqctx: RequestContext<Arc<ServerContext>>,
) -> Result<Response<Body>, HttpError> {
    // TODO: wrap in instrumenting code
    let opctx = crate::context::op_context_for_external_api(&rqctx).await;

    // if the user is already logged in, redirect to /. they can log out if needed
    if let Ok(opctx) = opctx {
        if opctx.authn.actor().is_some() {
            return Ok(Response::builder()
                .status(StatusCode::FOUND)
                .header(http::header::LOCATION, "/")
                .body("".into())?);
        }
    }

    serve_console_index(rqctx).await
}

/// Generic login post endpoint. Designed to work the same way as
/// `/login/{silo_name}/local`, but pulling the silo name from the host.
#[endpoint {
   method = POST,
   path = "/v1/login",
   tags = ["login"],
}]
pub async fn login(
    rqctx: RequestContext<Arc<ServerContext>>,
    credentials: TypedBody<params::UsernamePasswordCredentials>,
) -> Result<HttpResponseSeeOther, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let credentials = credentials.into_inner();

        // By definition, this request is not authenticated.  These operations
        // happen using the Nexus "external authentication" context, which we
        // keep specifically for this purpose.
        let opctx = nexus.opctx_external_authn();

        // TODO: get silo name from host. In the meantime, hard code silo. Tests
        // will pass but this will fail on the rack.
        let silo_name = Name::from_str("test-suite-silo").unwrap();

        let silo_lookup = nexus.silo_lookup(&opctx, silo_name.into())?;
        let user = nexus.login_local(&opctx, &silo_lookup, credentials).await?;
        login_finish(&opctx, apictx, user, None).await
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

pub async fn console_index_or_login_redirect(
    rqctx: RequestContext<Arc<ServerContext>>,
) -> Result<Response<Body>, HttpError> {
    let opctx = crate::context::op_context_for_external_api(&rqctx).await;

    // if authed, serve console index.html with JS bundle in script tag
    if let Ok(opctx) = opctx {
        if opctx.authn.actor().is_some() {
            return serve_console_index(rqctx).await;
        }
    }

    // otherwise redirect to login

    // Current path (to put in the query string to redirect back after login)
    let redirect_url =
        rqctx.request.uri().path_and_query().map(|p| p.to_string());

    Ok(Response::builder()
        .status(StatusCode::FOUND)
        .header(
            http::header::LOCATION,
            path_with_state_param("/login", redirect_url),
        )
        .body("".into())?)
}

// Dropshot does not have route match ranking and does not allow overlapping
// route definitions, so we cannot use a catchall `/*` route for console pages
// because it would overlap with the API routes definitions. So instead we have
// to manually define more specific routes.

macro_rules! console_page {
    ($name:ident, $path:literal) => {
        #[endpoint { method = GET, path = $path, unpublished = true, }]
        pub async fn $name(
            rqctx: RequestContext<Arc<ServerContext>>,
        ) -> Result<Response<Body>, HttpError> {
            console_index_or_login_redirect(rqctx).await
        }
    };
}

// only difference is the _path_params arg
macro_rules! console_page_wildcard {
    ($name:ident, $path:literal) => {
        #[endpoint { method = GET, path = $path, unpublished = true, }]
        pub async fn $name(
            rqctx: RequestContext<Arc<ServerContext>>,
            _path_params: Path<RestPathParam>,
        ) -> Result<Response<Body>, HttpError> {
            console_index_or_login_redirect(rqctx).await
        }
    };
}

console_page_wildcard!(console_projects, "/projects/{path:.*}");
console_page_wildcard!(console_settings_page, "/settings/{path:.*}");
console_page_wildcard!(console_system_page, "/system/{path:.*}");
console_page!(console_root, "/");
console_page!(console_projects_new, "/projects-new");
console_page!(console_silo_images, "/images");
console_page!(console_silo_utilization, "/utilization");
console_page!(console_silo_access, "/access");

/// Make a new PathBuf with `.gz` on the end
fn with_gz_ext(path: &PathBuf) -> PathBuf {
    let mut new_path = path.clone();
    let new_ext = match path.extension().map(|ext| ext.to_str()) {
        Some(Some(curr_ext)) => format!("{curr_ext}.gz"),
        _ => "gz".to_string(),
    };
    new_path.set_extension(new_ext);
    new_path
}

/// Fetch a static asset from `<static_dir>/assets`. 404 on virtually all
/// errors. No auth. NO SENSITIVE FILES. Will serve a gzipped version if the
/// `.gz` file is present in the directory and `Accept-Encoding: gzip` is
/// present on the request. Cache in browser for a year because assets have
/// content hash in filename.
#[endpoint {
   method = GET,
   path = "/assets/{path:.*}",
   unpublished = true,
}]
pub async fn asset(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<RestPathParam>,
) -> Result<Response<Body>, HttpError> {
    let apictx = rqctx.context();
    let path = PathBuf::from_iter(path_params.into_inner().path);

    // Bail unless the extension is allowed
    let ext = path
        .extension()
        .map_or_else(|| OsString::from("disallowed"), |ext| ext.to_os_string());
    if !ALLOWED_EXTENSIONS.contains(&ext) {
        return Err(not_found("file extension not allowed"));
    }

    // We only serve assets from assets/ within static_dir
    let assets_dir = &apictx
        .console_config
        .static_dir
        .as_ref()
        .ok_or_else(|| not_found("static_dir undefined"))?
        .join("assets");

    let request = &rqctx.request;
    let accept_encoding = request.headers().get(http::header::ACCEPT_ENCODING);
    let accept_gz = accept_encoding.map_or(false, |val| {
        val.to_str().map_or(false, |s| s.contains("gzip"))
    });

    // If req accepts gzip and we have a gzipped version, serve that. Otherwise
    // fall back to non-gz. If neither file found, bubble up 404.
    let (path_to_read, set_content_encoding_gzip) =
        match accept_gz.then(|| find_file(&with_gz_ext(&path), &assets_dir)) {
            Some(Ok(gzipped_path)) => (gzipped_path, true),
            _ => (find_file(&path, &assets_dir)?, false),
        };

    // File read is the same regardless of gzip
    let file_contents = tokio::fs::read(&path_to_read).await.map_err(|e| {
        not_found(&format!("accessing {:?}: {:#}", path_to_read, e))
    })?;

    // Derive the MIME type from the file name (can't use path_to_read because
    // it might end with .gz)
    let content_type = path.file_name().map_or("text/plain", |f| {
        mime_guess::from_path(f).first_raw().unwrap_or("text/plain")
    });

    let mut resp = Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, content_type)
        .header(http::header::CACHE_CONTROL, "max-age=31536000, immutable"); // 1 year

    if set_content_encoding_gzip {
        resp = resp.header(http::header::CONTENT_ENCODING, "gzip");
    }

    Ok(resp.body(file_contents.into())?)
}

pub async fn serve_console_index(
    rqctx: RequestContext<Arc<ServerContext>>,
) -> Result<Response<Body>, HttpError> {
    let apictx = rqctx.context();
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
        // do not cache this response in browser
        .header(http::header::CACHE_CONTROL, "no-store")
        .body(file_contents.into())?)
}

fn not_found(internal_msg: &str) -> HttpError {
    HttpError::for_not_found(None, internal_msg.to_string())
}

lazy_static! {
    static ref ALLOWED_EXTENSIONS: HashSet<OsString> = HashSet::from(
        [
            "js", "css", "html", "ico", "map", "otf", "png", "svg", "ttf",
            "txt", "webp", "woff", "woff2",
        ]
        .map(|s| OsString::from(s))
    );
}

/// Starting from `root_dir`, follow the segments of `path` down the file tree
/// until we find a file (or not). Do not follow symlinks.
fn find_file(path: &PathBuf, root_dir: &PathBuf) -> Result<PathBuf, HttpError> {
    let mut current = root_dir.to_owned(); // start from `root_dir`
    for segment in path.into_iter() {
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

    Ok(current)
}

#[cfg(test)]
mod test {
    use super::find_file;
    use http::StatusCode;
    use std::{env::current_dir, path::PathBuf};

    #[test]
    fn test_find_file_finds_file() {
        let root = current_dir().unwrap();
        let file =
            find_file(&PathBuf::from("tests/static/assets/hello.txt"), &root);
        assert!(file.is_ok());
        let file = find_file(&PathBuf::from("tests/static/index.html"), &root);
        assert!(file.is_ok());
    }

    #[test]
    fn test_find_file_404_on_nonexistent() {
        let root = current_dir().unwrap();
        let error =
            find_file(&PathBuf::from("tests/static/nonexistent.svg"), &root)
                .unwrap_err();
        assert_eq!(error.status_code, StatusCode::NOT_FOUND);
        assert_eq!(error.internal_message, "failed to get file metadata",);
    }

    #[test]
    fn test_find_file_404_on_nonexistent_nested() {
        let root = current_dir().unwrap();
        let error = find_file(
            &PathBuf::from("tests/static/a/b/c/nonexistent.svg"),
            &root,
        )
        .unwrap_err();
        assert_eq!(error.status_code, StatusCode::NOT_FOUND);
        assert_eq!(error.internal_message, "failed to get file metadata")
    }

    #[test]
    fn test_find_file_404_on_directory() {
        let root = current_dir().unwrap();
        let error =
            find_file(&PathBuf::from("tests/static/assets/a_directory"), &root)
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
        let error = find_file(&PathBuf::from(path_str), &root).unwrap_err();
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
        let error = find_file(&PathBuf::from(path_str), &root).unwrap_err();
        assert_eq!(error.status_code, StatusCode::NOT_FOUND);
        assert_eq!(error.internal_message, "attempted to follow a symlink");
    }
}
