// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Entrypoints for the OAuth 2.0 Device Authorization Grant flow.
//!
//! These are endpoints used by the API client per se (e.g., the CLI),
//! *not* the user of that client (e.g., an Oxide rack operator). They
//! are for requesting access tokens that will be managed and used by
//! the client to make other API requests.

use super::console_api::console_index_or_login_redirect;
use super::views::DeviceAccessTokenGrant;
use crate::app::external_endpoints::authority_for_request;
use crate::ApiContext;
use dropshot::{
    endpoint, HttpError, HttpResponseUpdatedNoContent, RequestContext,
    TypedBody,
};
use http::{header, Response, StatusCode};
use hyper::Body;
use nexus_db_queries::db::model::DeviceAccessToken;
use omicron_common::api::external::InternalContext;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
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
pub struct DeviceAuthRequest {
    pub client_id: Uuid,
}

/// Start an OAuth 2.0 Device Authorization Grant
///
/// This endpoint is designed to be accessed from an *unauthenticated*
/// API client. It generates and records a `device_code` and `user_code`
/// which must be verified and confirmed prior to a token being granted.
#[endpoint {
    method = POST,
    path = "/device/auth",
    content_type = "application/x-www-form-urlencoded",
    tags = ["hidden"], // "token"
}]
pub(crate) async fn device_auth_request(
    rqctx: RequestContext<ApiContext>,
    params: TypedBody<DeviceAuthRequest>,
) -> Result<Response<Body>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.context.nexus;
    let params = params.into_inner();
    let handler = async {
        let opctx = nexus.opctx_external_authn();
        let authority = authority_for_request(&rqctx.request);
        let host = match &authority {
            Ok(host) => host.as_str(),
            Err(error) => {
                return build_oauth_response(
                    StatusCode::BAD_REQUEST,
                    &serde_json::json!({
                        "error": "invalid_request",
                        "error_description": error,
                    }),
                )
            }
        };

        let model =
            nexus.device_auth_request_create(&opctx, params.client_id).await?;
        build_oauth_response(
            StatusCode::OK,
            &model.into_response(rqctx.server.using_tls(), host),
        )
    };
    apictx
        .context
        .external_latencies
        .instrument_dropshot_handler(&rqctx, handler)
        .await
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct DeviceAuthVerify {
    pub user_code: String,
}

/// Verify an OAuth 2.0 Device Authorization Grant
///
/// This endpoint should be accessed in a full user agent (e.g.,
/// a browser). If the user is not logged in, we redirect them to
/// the login page and use the `state` parameter to get them back
/// here on completion. If they are logged in, serve up the console
/// verification page so they can verify the user code.
#[endpoint {
    method = GET,
    path = "/device/verify",
    unpublished = true,
}]
pub(crate) async fn device_auth_verify(
    rqctx: RequestContext<ApiContext>,
) -> Result<Response<Body>, HttpError> {
    console_index_or_login_redirect(rqctx).await
}

#[endpoint {
    method = GET,
    path = "/device/success",
    unpublished = true,
}]
pub(crate) async fn device_auth_success(
    rqctx: RequestContext<ApiContext>,
) -> Result<Response<Body>, HttpError> {
    console_index_or_login_redirect(rqctx).await
}

/// Confirm an OAuth 2.0 Device Authorization Grant
///
/// This endpoint is designed to be accessed by the user agent (browser),
/// not the client requesting the token. So we do not actually return the
/// token here; it will be returned in response to the poll on `/device/token`.
#[endpoint {
    method = POST,
    path = "/device/confirm",
    tags = ["hidden"], // "token"
}]
pub(crate) async fn device_auth_confirm(
    rqctx: RequestContext<ApiContext>,
    params: TypedBody<DeviceAuthVerify>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.context.nexus;
    let params = params.into_inner();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let &actor = opctx.authn.actor_required().internal_context(
            "creating new device auth session for current user",
        )?;
        let _token = nexus
            .device_auth_request_verify(
                &opctx,
                params.user_code,
                actor.actor_id(),
            )
            .await?;
        Ok(HttpResponseUpdatedNoContent())
    };
    apictx
        .context
        .external_latencies
        .instrument_dropshot_handler(&rqctx, handler)
        .await
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct DeviceAccessTokenRequest {
    pub grant_type: String,
    pub device_code: String,
    pub client_id: Uuid,
}

#[derive(Debug)]
pub enum DeviceAccessTokenResponse {
    Granted(DeviceAccessToken),
    Pending,
    #[allow(dead_code)]
    Denied,
}

/// Request a device access token
///
/// This endpoint should be polled by the client until the user code
/// is verified and the grant is confirmed.
#[endpoint {
    method = POST,
    path = "/device/token",
    content_type = "application/x-www-form-urlencoded",
    tags = ["hidden"], // "token"
}]
pub(crate) async fn device_access_token(
    rqctx: RequestContext<ApiContext>,
    params: TypedBody<DeviceAccessTokenRequest>,
) -> Result<Response<Body>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.context.nexus;
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
        use DeviceAccessTokenResponse::*;
        match nexus
            .device_access_token_fetch(
                &opctx,
                params.client_id,
                params.device_code,
            )
            .await
        {
            Ok(response) => match response {
                Granted(token) => build_oauth_response(
                    StatusCode::OK,
                    &DeviceAccessTokenGrant::from(token),
                ),
                Pending => build_oauth_response(
                    StatusCode::BAD_REQUEST,
                    &serde_json::json!({
                        "error": "authorization_pending"
                    }),
                ),
                Denied => build_oauth_response(
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
    apictx
        .context
        .external_latencies
        .instrument_dropshot_handler(&rqctx, handler)
        .await
}
