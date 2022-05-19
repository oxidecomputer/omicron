// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Handler functions (entrypoints) for client-related routes.
//!
//! These are functions of the client per se (e.g., the CLI), not
//! the _user_ of that client, with which most of the external API
//! is concerned. They are currently only for requesting access
//! tokens that will be managed and used by the client to make other
//! API requests.

use super::console_api::{get_login_url, serve_console_index};
use super::views::{ClientAuthentication, ClientTokenGrant};
use crate::context::OpContext;
use crate::db::model::ClientToken;
use crate::ServerContext;
use dropshot::{
    endpoint, HttpError, HttpResponseOk, Query, RequestContext, TypedBody,
};
use http::{header, Response, StatusCode};
use hyper::Body;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_urlencoded;
use std::sync::Arc;
use uuid::Uuid;

// Token granting à la RFC 8628 (OAuth 2.0 Device Authorization Grant)

/// OAuth 2.0 error responses use 400 (Bad Request) with specific `error`
/// parameter values to indicate protocol errors (see RFC 6749 §5.2).
/// This is different from Dropshot's error `message` parameter, so we
/// need a custom response builder.
fn build_oauth_response<T>(
    status: StatusCode,
    body: &T,
) -> Result<Response<Body>, HttpError>
where
    T: ?Sized + Serialize,
{
    let body = serde_json::to_string(body)
        .map_err(|e| HttpError::for_internal_error(e.to_string()))?;
    Ok(Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(body.into())?)
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct AuthenticateParams {
    pub client_id: Uuid,
}

/// Start an OAuth 2.0 Device Authorization Grant
///
/// This endpoint is designed to be accessed from an unauthenticated
/// API client. It generates and records a `device_code` and `user_code`
/// which must be verified and confirmed prior to a token being granted.
#[endpoint {
    method = POST,
    path = "/client/authenticate",
    tags = ["hidden"], // "token"
}]
pub async fn client_authenticate(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    params: TypedBody<AuthenticateParams>,
) -> Result<HttpResponseOk<ClientAuthentication>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let params = params.into_inner();
    let handler = async {
        // TODO: use build_oauth_response
        let opctx = nexus.opctx_external_authn();
        let model = nexus.client_authenticate(&opctx, params.client_id).await?;
        let request = rqctx.request.lock().await;
        let host = request
            .headers()
            .get(header::HOST)
            .ok_or_else(|| {
                HttpError::for_bad_request(
                    None,
                    String::from("missing Host header in request"),
                )
            })?
            .to_str()
            .map_err(|e| {
                HttpError::for_bad_request(
                    None,
                    format!("could not decode Host header: {}", e),
                )
            })?;
        let view = ClientAuthentication::from_model(model, host);
        Ok(HttpResponseOk(dbg!(view)))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ClientVerificationParams {
    pub user_code: String,
}

/// Verify an OAuth 2.0 Device Authorization Grant
///
/// Verify the user code and redirect to the login page. This endpoint
/// should be accessed in a full user agent (e.g., a browser). If the
/// user is not logged in, we redirect them to the login page and use
/// the `state` parameter to get them back here on completion.
#[endpoint {
    method = GET,
    path = "/client/verify",
    tags = ["hidden"], // "token"
}]
pub async fn client_verify(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    params: Query<ClientVerificationParams>,
) -> Result<Response<Body>, HttpError> {
    // If the user is authenticated, serve the console verification page.
    if let Ok(opctx) = OpContext::for_external_api(&rqctx).await {
        if opctx.authn.actor().is_some() {
            return serve_console_index(rqctx.context()).await;
        }
    }

    // Otherwise, redirect for authentication.
    let params = params.into_inner();
    let state_params = serde_urlencoded::to_string(serde_json::json!({
        "user_code": params.user_code
    }))
    .map_err(|e| HttpError::for_internal_error(e.to_string()))?;
    let state = Some(format!("/client/verify?{}", state_params));
    Ok(Response::builder()
        .status(StatusCode::FOUND)
        .header(http::header::LOCATION, get_login_url(state))
        .body("".into())?)
}

/// Confirm an OAuth 2.0 Device Authorization Grant
///
/// This endpoint is designed to be accessed by the user agent (browser),
/// not the client requesting the token. So we do not actually return the
/// token here; it will be returned in response to the poll on `/client/token`.
#[endpoint {
    method = POST,
    path = "/client/confirm",
    tags = ["hidden"], // "token"
}]
pub async fn client_confirm(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    params: TypedBody<ClientVerificationParams>,
) -> Result<HttpResponseOk<()>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let params = params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let &actor = opctx.authn.actor_required()?;
        let _token = nexus
            .client_verified(&opctx, params.user_code, actor.actor_id())
            .await?;
        Ok(HttpResponseOk(()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct TokenRequestParams {
    pub grant_type: String,
    pub device_code: String,
    pub client_id: Uuid,
}

#[derive(Debug)]
pub enum TokenResponse {
    Granted(ClientToken),
    Pending,
    Denied,
}

/// Finish an OAuth 2.0 Device Authorization Grant
///
/// This endpoint should be polled by the client until the user code
/// is verified and the grant is confirmed.
#[endpoint {
    method = POST,
    path = "/client/token",
    tags = ["hidden"], // "token"
}]
pub async fn client_grant_token(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    params: TypedBody<TokenRequestParams>,
) -> Result<Response<Body>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let params = params.into_inner();
    let handler = async {
        // RFC 8628 §3.4
        if params.grant_type != "urn:ietf:params:oauth:grant-type:device_code" {
            return build_oauth_response(
                StatusCode::BAD_REQUEST,
                &serde_json::json!({
                    "error": "unsupported_grant_type"
                }),
            );
        }

        // RFC 8628 §3.5
        let opctx = nexus.opctx_external_authn();
        match nexus
            .client_get_token(&opctx, params.client_id, params.device_code)
            .await
        {
            Ok(response) => match response {
                TokenResponse::Granted(token) => build_oauth_response(
                    StatusCode::OK,
                    &ClientTokenGrant::from(token),
                ),
                TokenResponse::Pending => build_oauth_response(
                    StatusCode::BAD_REQUEST,
                    &serde_json::json!({
                        "error": "authorization_pending"
                    }),
                ),
                TokenResponse::Denied => build_oauth_response(
                    StatusCode::BAD_REQUEST,
                    &serde_json::json!({
                        "error": "access_denied"
                    }),
                ),
            },
            Err(error) => build_oauth_response(
                StatusCode::BAD_REQUEST,
                &serde_json::json!({
                    "error": "invalid_request",
                    "error_description": format!("{}", error),
                }),
            ),
        }
    };
    // TODO: instrumentation doesn't work because we use `Response<Body>`
    //apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
    handler.await
}
