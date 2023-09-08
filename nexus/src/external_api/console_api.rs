// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Handler functions (entrypoints) for console-related routes.
//!
//! This was originally conceived as a separate dropshot server from the
//! external API, but in order to avoid CORS issues for now, we are serving
//! these routes directly from the external API.

use crate::ServerContext;
use anyhow::Context;
use dropshot::{
    endpoint, http_response_found, http_response_see_other, HttpError,
    HttpResponseFound, HttpResponseHeaders, HttpResponseSeeOther,
    HttpResponseUpdatedNoContent, Path, Query, RequestContext,
};
use http::{header, Response, StatusCode, Uri};
use hyper::Body;
use lazy_static::lazy_static;
use mime_guess;
use nexus_db_model::AuthenticationMode;
use nexus_db_queries::authn::silos::IdentityProviderType;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::{
    authn::external::{
        cookies::Cookies,
        session_cookie::{
            clear_session_cookie_header_value, session_cookie_header_value,
            SessionStore, SESSION_COOKIE_COOKIE_NAME,
        },
    },
    db::identity::Asset,
};
use nexus_types::external_api::params;
use nexus_types::identity::Resource;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::{DataPageParams, Error, NameOrId};
use parse_display::Display;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_urlencoded;
use std::num::NonZeroU32;
use std::str::FromStr;
use std::{collections::HashSet, ffi::OsString, path::PathBuf, sync::Arc};

// -----------------------------------------------------
// High-level overview of how login works in the console
// -----------------------------------------------------
//
// There's a lot of noise in this file, so here is the big picture. A user can
// get sent to login in two ways in the console:
//
// 1. Navigate an auth-gated console page (e.g., /projects) directly while
//    logged out. This goes through `console_index_or_login_redirect` and
//    therefore results in a redirect straight to either
//    /login/{silo}/local?redirect_uri={} or
//    /login/{silo}/saml/{provider}?redirect_uri={} depending on the silo's
//    authn mode. Nexus takes the path the user was trying to hit and sticks it
//    in a `redirect_uri` query param.
// 2. Hit a 401 on a background API call while already in the console (for
//    example, if session expires while in use). In that case, the console will
//    navigate to `/login?redirect_uri={current_path}`, which will respond with
//    a redirect to local or SAML login as above.
//
// Local login is very simple. We show a login form, the username and password
// are POSTed to the API, and on success, the console pulls `redirect_uri` out
// of the query params and navigates to that route client-side.
//
// SAML is more complicated. /login/{silo}/saml/{provider}?redirect_uri={} shows
// a page with a link to /login/{silo}/saml/{provider}/redirect?redirect_uri={}
// (note the /redirect), which redirects to the IdP  with `redirect_uri` encoded
// in a RelayState query param). On successful login in the IdP, the IdP will
// POST /login/{silo}/saml/{provider} with a body including that redirect_uri,
// so that on success, we can redirect to the original target page.

// -------------------------------
// Detailed overview of SAML login
// -------------------------------
//
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
// anything - Nexus currently encodes a redirect_uri so that when SAML login
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
// user provision type). After that, users will be redirected to the `redirect_uri`
// in the relay state, or to `/organizations`.
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
    pub silo_name: nexus_db_queries::db::model::Name,
    pub provider_name: nexus_db_queries::db::model::Name,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct RelayState {
    pub redirect_uri: Option<RelativeUri>,
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

/// SAML login console page (just a link to the IdP)
#[endpoint {
   method = GET,
   path = "/login/{silo_name}/saml/{provider_name}",
   tags = ["login"],
   unpublished = true,
}]
pub(crate) async fn login_saml_begin(
    rqctx: RequestContext<Arc<ServerContext>>,
    _path_params: Path<LoginToProviderPathParam>,
    _query_params: Query<LoginUrlQuery>,
) -> Result<Response<Body>, HttpError> {
    serve_console_index(rqctx.context()).await
}

/// Get a redirect straight to the IdP
///
/// Console uses this to avoid having to ask the API anything about the IdP. It
/// already knows the IdP name from the path, so it can just link to this path
/// and rely on Nexus to redirect to the actual IdP.
#[endpoint {
   method = GET,
   path = "/login/{silo_name}/saml/{provider_name}/redirect",
   tags = ["login"],
   unpublished = true,
}]
pub(crate) async fn login_saml_redirect(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<LoginToProviderPathParam>,
    query_params: Query<LoginUrlQuery>,
) -> Result<HttpResponseFound, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let path_params = path_params.into_inner();

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
                let redirect_uri = query_params.into_inner().redirect_uri;
                let relay_state =
                    RelayState { redirect_uri }.to_encoded().map_err(|e| {
                        HttpError::for_internal_error(format!(
                            "encoding relay state failed: {}",
                            e
                        ))
                    })?;

                let sign_in_url = saml_identity_provider
                    .sign_in_url(Some(relay_state))
                    .map_err(|e| {
                        HttpError::for_internal_error(e.to_string())
                    })?;

                http_response_found(sign_in_url)
            }
        }
    };

    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Authenticate a user via SAML
#[endpoint {
   method = POST,
   path = "/login/{silo_name}/saml/{provider_name}",
   tags = ["login"],
}]
pub(crate) async fn login_saml(
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

        let relay_state =
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

        let session = create_session(opctx, apictx, user).await?;
        let next_url = relay_state
            .and_then(|r| r.redirect_uri)
            .map(|u| u.to_string())
            .unwrap_or_else(|| "/".to_string());
        let mut response = http_response_see_other(next_url)?;

        {
            let headers = response.headers_mut();
            let cookie = session_cookie_header_value(
                &session.token,
                // use absolute timeout even though session might idle out first.
                // browser expiration is mostly for convenience, as the API will
                // reject requests with an expired session regardless
                apictx.session_absolute_timeout(),
                apictx.external_tls_enabled,
            )?;
            headers.append(header::SET_COOKIE, cookie);
        }
        Ok(response)
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

#[derive(Deserialize, JsonSchema)]
pub struct LoginPathParam {
    pub silo_name: nexus_db_queries::db::model::Name,
}

#[endpoint {
   method = GET,
   path = "/login/{silo_name}/local",
   tags = ["login"],
   unpublished = true,
}]
pub(crate) async fn login_local_begin(
    rqctx: RequestContext<Arc<ServerContext>>,
    _path_params: Path<LoginPathParam>,
    _query_params: Query<LoginUrlQuery>,
) -> Result<Response<Body>, HttpError> {
    // TODO: figure out why instrumenting doesn't work
    // let apictx = rqctx.context();
    // let handler = async { serve_console_index(rqctx.context()).await };
    // apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
    serve_console_index(rqctx.context()).await
}

/// Authenticate a user via username and password
#[endpoint {
   method = POST,
   path = "/v1/login/{silo_name}/local",
   tags = ["login"],
}]
pub(crate) async fn login_local(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<LoginPathParam>,
    credentials: dropshot::TypedBody<params::UsernamePasswordCredentials>,
) -> Result<HttpResponseHeaders<HttpResponseUpdatedNoContent>, HttpError> {
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

        let session = create_session(opctx, apictx, user).await?;
        let mut response =
            HttpResponseHeaders::new_unnamed(HttpResponseUpdatedNoContent());

        {
            let headers = response.headers_mut();
            let cookie = session_cookie_header_value(
                &session.token,
                // use absolute timeout even though session might idle out first.
                // browser expiration is mostly for convenience, as the API will
                // reject requests with an expired session regardless
                apictx.session_absolute_timeout(),
                apictx.external_tls_enabled,
            )?;
            headers.append(header::SET_COOKIE, cookie);
        }
        Ok(response)
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

async fn create_session(
    opctx: &OpContext,
    apictx: &ServerContext,
    user: Option<nexus_db_queries::db::model::SiloUser>,
) -> Result<nexus_db_queries::db::model::ConsoleSession, HttpError> {
    let nexus = &apictx.nexus;
    let session = match user {
        Some(user) => nexus.session_create(&opctx, user.id()).await?,
        None => Err(Error::Unauthenticated {
            internal_message: String::from(
                "no matching user found or credentials were not valid",
            ),
        })?,
    };
    Ok(session)
}

/// Log user out of web console by deleting session on client and server
#[endpoint {
   // important for security that this be a POST despite the empty req body
   method = POST,
   path = "/v1/logout",
   tags = ["hidden"],
}]
pub(crate) async fn logout(
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
                clear_session_cookie_header_value(apictx.external_tls_enabled)?,
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

/// This is meant as a security feature. We want to ensure we never redirect to
/// a URI on a different host.
#[derive(Serialize, Deserialize, Debug, JsonSchema, Clone, Display)]
#[serde(try_from = "String")]
#[display("{0}")]
pub struct RelativeUri(String);

impl FromStr for RelativeUri {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from(s.to_string())
    }
}

impl TryFrom<Uri> for RelativeUri {
    type Error = String;

    fn try_from(uri: Uri) -> Result<Self, Self::Error> {
        if uri.host().is_none() && uri.scheme().is_none() {
            Ok(Self(uri.to_string()))
        } else {
            Err(format!("\"{}\" is not a relative URI", uri))
        }
    }
}

impl TryFrom<String> for RelativeUri {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.parse::<Uri>()
            .map_err(|_| format!("\"{}\" is not a relative URI", s))
            .and_then(|uri| Self::try_from(uri))
    }
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct LoginUrlQuery {
    redirect_uri: Option<RelativeUri>,
}

/// Generate URI to the appropriate login form for this Silo. Optional
/// `redirect_uri` represents the URL to send the user back to after successful
/// login, and is included in `state` query param if present
async fn get_login_url(
    rqctx: &RequestContext<Arc<ServerContext>>,
    redirect_uri: Option<RelativeUri>,
) -> Result<String, Error> {
    let nexus = &rqctx.context().nexus;
    let endpoint = nexus.endpoint_for_request(rqctx)?;
    let silo = endpoint.silo();

    let login_uri = if silo.authentication_mode == AuthenticationMode::Local {
        format!("/login/{}/local", silo.name())
    } else {
        // List the available identity providers and pick an arbitrary one.
        // It might be nice to have some policy for choosing which one is used
        // here.
        let opctx = nexus.opctx_external_authn();
        let silo_lookup = nexus.silo_lookup(opctx, NameOrId::Id(silo.id()))?;
        let idps = nexus
            .identity_provider_list(
                opctx,
                &silo_lookup,
                &PaginatedBy::Name(DataPageParams {
                    marker: None,
                    direction: dropshot::PaginationOrder::Ascending,
                    limit: NonZeroU32::try_from(2).unwrap(),
                }),
            )
            .await?;

        if idps.is_empty() {
            let error = Error::invalid_request(
                "no identity providers are configured for Silo",
            );
            warn!(&rqctx.log, "{}", error; "silo_id" => ?silo.id());
            return Err(error);
        }

        if idps.len() > 1 {
            warn!(
                &rqctx.log,
                "found more than one IdP for Silo";
                "silo_id" => ?silo.id()
            );
        }

        format!(
            "/login/{}/saml/{}",
            silo.name(),
            idps.into_iter().next().unwrap().name()
        )
    };

    // Stick redirect_url into the state param and URL encode it so it can be
    // used as a query string. We assume it's not already encoded.
    let query_data = LoginUrlQuery { redirect_uri };

    Ok(match serde_urlencoded::to_string(query_data) {
        // only put the ? in front if there's something there
        Ok(encoded) if !encoded.is_empty() => format!("{login_uri}?{encoded}"),
        // redirect_url is either None or not url-encodable for some reason
        _ => login_uri,
    })
}

/// Redirect to a login page for the current Silo (if that can be determined)
#[endpoint {
   method = GET,
   path = "/login",
   unpublished = true,
}]
pub(crate) async fn login_begin(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<LoginUrlQuery>,
) -> Result<HttpResponseFound, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let query = query_params.into_inner();
        let login_url = get_login_url(&rqctx, query.redirect_uri).await?;
        http_response_found(login_url)
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

pub(crate) async fn console_index_or_login_redirect(
    rqctx: RequestContext<Arc<ServerContext>>,
) -> Result<Response<Body>, HttpError> {
    let opctx = crate::context::op_context_for_external_api(&rqctx).await;

    // if authed, serve console index.html with JS bundle in script tag
    if let Ok(opctx) = opctx {
        if opctx.authn.actor().is_some() {
            return serve_console_index(rqctx.context()).await;
        }
    }

    // otherwise redirect to idp

    // Put the current URI in the query string to redirect back to after login.
    let redirect_uri = rqctx
        .request
        .uri()
        .path_and_query()
        .map(|p| RelativeUri(p.to_string()));
    let login_url = get_login_url(&rqctx, redirect_uri).await?;

    Ok(Response::builder()
        .status(StatusCode::FOUND)
        .header(http::header::LOCATION, login_url)
        .body("".into())?)
}

// Dropshot does not have route match ranking and does not allow overlapping
// route definitions, so we cannot use a catchall `/*` route for console pages
// because it would overlap with the API routes definitions. So instead we have
// to manually define more specific routes.

macro_rules! console_page {
    ($name:ident, $path:literal) => {
        #[endpoint { method = GET, path = $path, unpublished = true, }]
        pub(crate) async fn $name(
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
        pub(crate) async fn $name(
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
pub(crate) async fn asset(
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

pub(crate) async fn serve_console_index(
    apictx: &ServerContext,
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
    use super::{find_file, RelativeUri};
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

    #[test]
    fn test_relative_uri() {
        let good = ["/", "/abc", "/abc/def"];
        for g in good.iter() {
            assert!(RelativeUri::try_from(g.to_string()).is_ok());
        }

        let bad = ["", "abc", "example.com", "http://example.com"];
        for b in bad.iter() {
            assert!(RelativeUri::try_from(b.to_string()).is_err());
        }
    }
}
