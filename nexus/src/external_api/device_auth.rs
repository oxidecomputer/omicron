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
use dropshot::Body;
use dropshot::{
    HttpError, HttpResponseUpdatedNoContent, RequestContext, TypedBody,
};
use http::{header, Response, StatusCode};
use nexus_db_queries::db::model::DeviceAccessToken;
use nexus_types::external_api::params;
use omicron_common::api::external::InternalContext;
use serde::Serialize;

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

pub(crate) async fn device_auth_request(
    rqctx: RequestContext<ApiContext>,
    params: TypedBody<params::DeviceAuthRequest>,
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

pub(crate) async fn device_auth_verify(
    rqctx: RequestContext<ApiContext>,
) -> Result<Response<Body>, HttpError> {
    console_index_or_login_redirect(rqctx).await
}

pub(crate) async fn device_auth_success(
    rqctx: RequestContext<ApiContext>,
) -> Result<Response<Body>, HttpError> {
    console_index_or_login_redirect(rqctx).await
}

pub(crate) async fn device_auth_confirm(
    rqctx: RequestContext<ApiContext>,
    params: TypedBody<params::DeviceAuthVerify>,
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

#[derive(Debug)]
pub enum DeviceAccessTokenResponse {
    Granted(DeviceAccessToken),
    Pending,
    #[allow(dead_code)]
    Denied,
}

pub(crate) async fn device_access_token(
    rqctx: RequestContext<ApiContext>,
    params: params::DeviceAccessTokenRequest,
) -> Result<Response<Body>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.context.nexus;
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
