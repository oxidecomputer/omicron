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
    endpoint, http_response_found, http_response_see_other, HttpError,
    HttpResponseFound, HttpResponseHeaders, HttpResponseOk,
    HttpResponseSeeOther, HttpResponseUpdatedNoContent, Path, Query,
    RequestContext, ResultsPage, TypedBody,
};
use http::{header, Response, StatusCode};
use hyper::Body;
use lazy_static::lazy_static;
use mime_guess;
use omicron_common::api::external::http_pagination::{
    data_page_params_for, PaginatedById, ScanById, ScanParams,
};
use omicron_common::api::external::Error;
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
pub async fn login_spoof(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    params: TypedBody<SpoofLoginBody>,
) -> Result<HttpResponseHeaders<HttpResponseUpdatedNoContent>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let params = params.into_inner();
        let user_id: Option<Uuid> = match params.username.as_str() {
            "privileged" => Some(USER_TEST_PRIVILEGED.id()),
            "unprivileged" => Some(USER_TEST_UNPRIVILEGED.id()),
            _ => None,
        };

        if user_id.is_none() {
            Err(Error::Unauthenticated {
                internal_message: String::from("unknown user specified"),
            })?;
        }

        let user_id = user_id.unwrap();

        // For now, we use the external authn context to create the session.
        // Once we have real SAML login, maybe we can cons up a real OpContext
        // for this user and use their own privileges to create the session.
        let authn_opctx = nexus.opctx_external_authn();
        let session = nexus.session_create(&authn_opctx, user_id).await?;

        let mut response =
            HttpResponseHeaders::new_unnamed(HttpResponseUpdatedNoContent());
        {
            let headers = response.headers_mut();
            headers.append(
                header::SET_COOKIE,
                http::HeaderValue::from_str(&session_cookie_header_value(
                    &session.token,
                    apictx.session_idle_timeout(),
                ))
                .map_err(|error| {
                    HttpError::for_internal_error(format!(
                        "unsupported cookie value: {:#}",
                        error
                    ))
                })?,
            );
        };
        Ok(response)
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
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
// `GET /system/silos/{silo_name}/identity_providers` in order to list available
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
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<LoginToProviderPathParam>,
) -> Result<HttpResponseFound, HttpError> {
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

/// Authenticate a user
///
/// Either receive a username and password, or some sort of identity provider
/// data (like a SAMLResponse). Use these to set the user's session cookie.
#[endpoint {
   method = POST,
   path = "/login/{silo_name}/saml/{provider_name}",
   tags = ["login"],
}]
pub async fn login_saml(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<LoginToProviderPathParam>,
    body_bytes: dropshot::UntypedBody,
) -> Result<HttpResponseSeeOther, HttpError> {
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
            Err(Error::Unauthenticated {
                internal_message: String::from(
                    "no matching user found or credentials were not valid",
                ),
            })?;
        }

        let user = user.unwrap();

        // always create a new console session if the user is POSTing here.
        let session = nexus.session_create(&opctx, user.id()).await?;

        let next_url = if let Some(relay_state) = &relay_state {
            if let Some(referer) = &relay_state.referer {
                referer.clone()
            } else {
                "/".to_string()
            }
        } else {
            "/".to_string()
        };

        debug!(
            &apictx.log,
            "successful login to silo {} using provider {}: authenticated \
            subject {} = user id {}",
            path_params.silo_name,
            path_params.provider_name,
            authenticated_subject.external_id,
            user.id(),
        );

        let mut response_with_headers = http_response_see_other(next_url)?;

        {
            let headers = response_with_headers.headers_mut();
            headers.append(
                header::SET_COOKIE,
                http::HeaderValue::from_str(&session_cookie_header_value(
                    &session.token,
                    apictx.session_idle_timeout(),
                ))
                .map_err(|error| {
                    HttpError::for_internal_error(format!(
                        "unsupported cookie value: {:#}",
                        error
                    ))
                })?,
            );
        }
        Ok(response_with_headers)
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
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
) -> Result<HttpResponseHeaders<HttpResponseUpdatedNoContent>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let opctx = OpContext::for_external_api(&rqctx).await;
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
                http::HeaderValue::from_str(
                    &clear_session_cookie_header_value(),
                )
                .map_err(|error| {
                    HttpError::for_internal_error(format!(
                        "unsupported cookie value: {:#}",
                        error
                    ))
                })?,
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
pub async fn login_spoof_begin(
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

/// Generate URI to IdP login form. Optional `redirect_url` represents the URL
/// to send the user back to after successful login, and is included in `state`
/// query param if present
fn get_login_url(redirect_url: Option<String>) -> String {
    // TODO: Once we have IdP integration, this will be a URL for the IdP login
    // page. For now we point to our own placeholder login page. When the user
    // is logged out and hits an auth-gated route, if there are multiple IdPs
    // and we don't known which one they want to use, we need to send them to a
    // page that will allow them to choose among discoverable IdPs. However,
    // there may be ways to give ourselves a hint about which one they want, for
    // example, by storing that info in a browser cookie when they log in. When
    // their session ends, we will not be able to look at the dead session to
    // find the silo or IdP (well, maybe we can but we probably shouldn't) but
    // we can look at the cookie and default to sending them to the IdP
    // indicated (though if they don't want that one we need to make sure they
    // can get to a different one). If there is no cookie, we send them to the
    // selector page. In any case, none of this is done here yet. We go to
    // /spoof_login no matter what.
    let login_uri = "/spoof_login";

    // Stick redirect_url into the state param and URL encode it so it can be
    // used as a query string. We assume it's not already encoded.
    let query_data = LoginUrlQuery { state: redirect_url };

    match serde_urlencoded::to_string(query_data) {
        // only put the ? in front if there's something there
        Ok(encoded) if !encoded.is_empty() => format!("{login_uri}?{encoded}"),
        // redirect_url is either None or not url-encodable for some reason
        _ => login_uri.to_string(),
    }
}

/// Redirect to IdP login URL
// Currently hard-coded to redirect to our own fake login form.
#[endpoint {
   method = GET,
   path = "/login",
   unpublished = true,
}]
pub async fn login_begin(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    query_params: Query<StateParam>,
) -> Result<HttpResponseFound, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let query = query_params.into_inner();
        let redirect_url = query.state.filter(|s| !s.trim().is_empty());
        let login_url = get_login_url(redirect_url);
        http_response_found(login_url)
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Fetch the user associated with the current session
#[endpoint {
   method = GET,
   path = "/session/me",
   tags = ["hidden"],
}]
pub async fn session_me(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
) -> Result<HttpResponseOk<views::User>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let handler = async {
        // We don't care about authentication method, as long as they are authed
        // as _somebody_. We could restrict this to session auth only, but it's
        // not clear what the advantage would be.
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let user = nexus.silo_user_fetch_self(&opctx).await?;
        Ok(HttpResponseOk(views::User {
            id: user.id(),
            display_name: user.external_id,
            silo_id: user.silo_id,
        }))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Fetch the silo groups of which the current user is a member
#[endpoint {
    method = GET,
    path = "/session/me/groups",
    tags = ["hidden"],
 }]
pub async fn session_me_groups(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    query_params: Query<PaginatedById>,
) -> Result<HttpResponseOk<ResultsPage<views::Group>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let query = query_params.into_inner();
    let handler = async {
        // We don't care about authentication method, as long as they are authed
        // as _somebody_. We could restrict this to session auth only, but it's
        // not clear what the advantage would be.
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let groups = nexus
            .silo_user_fetch_groups_for_self(
                &opctx,
                &data_page_params_for(&rqctx, &query)?,
            )
            .await?
            .into_iter()
            .map(|d| d.into())
            .collect();
        Ok(HttpResponseOk(ScanById::results_page(
            &query,
            groups,
            &|_, group: &views::Group| group.id,
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

pub async fn console_index_or_login_redirect(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
) -> Result<Response<Body>, HttpError> {
    let opctx = OpContext::for_external_api(&rqctx).await;

    // if authed, serve console index.html with JS bundle in script tag
    if let Ok(opctx) = opctx {
        if opctx.authn.actor().is_some() {
            return serve_console_index(rqctx.context()).await;
        }
    }

    // otherwise redirect to idp

    // Put the current URI in the query string to redirect back to after login.
    // Right now this is a relative path, which only works as long as we're
    // using the spoof login page, which is hosted by Nexus. Once we start
    // sending users to a real external IdP login page, this will need to be a
    // full URL.
    let redirect_url = rqctx
        .request
        .lock()
        .await
        .uri()
        .path_and_query()
        .map(|p| p.to_string());

    Ok(Response::builder()
        .status(StatusCode::FOUND)
        .header(http::header::LOCATION, get_login_url(redirect_url))
        .body("".into())?)
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
    console_index_or_login_redirect(rqctx).await
}

#[endpoint {
   method = GET,
   path = "/settings/{path:.*}",
   unpublished = true,
}]
pub async fn console_settings_page(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    _path_params: Path<RestPathParam>,
) -> Result<Response<Body>, HttpError> {
    console_index_or_login_redirect(rqctx).await
}

#[endpoint {
   method = GET,
   path = "/sys/{path:.*}",
   unpublished = true,
}]
pub async fn console_system_page(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    _path_params: Path<RestPathParam>,
) -> Result<Response<Body>, HttpError> {
    console_index_or_login_redirect(rqctx).await
}

#[endpoint {
   method = GET,
   path = "/",
   unpublished = true,
}]
pub async fn console_root(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
) -> Result<Response<Body>, HttpError> {
    console_index_or_login_redirect(rqctx).await
}

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
/// present on the request.
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

    let request = &rqctx.request.lock().await;
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
        .header(http::header::CACHE_CONTROL, cache_control_value(apictx));

    if set_content_encoding_gzip {
        resp = resp.header(http::header::CONTENT_ENCODING, "gzip");
    }

    Ok(resp.body(file_contents.into())?)
}

fn cache_control_value(apictx: &ServerContext) -> String {
    let max_age = apictx.console_config.cache_control_max_age.num_seconds();
    format!("max-age={max_age}")
}

pub async fn serve_console_index(
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
        .header(http::header::CACHE_CONTROL, cache_control_value(apictx))
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
