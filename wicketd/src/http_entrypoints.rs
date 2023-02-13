// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! HTTP entrypoint functions for wicketd

use crate::artifacts::TufRepositoryId;
use crate::mgs::GetInventoryResponse;
use crate::update_events::UpdateLog;
use crate::update_planner::UpdatePlanError;
use dropshot::endpoint;
use dropshot::ApiDescription;
use dropshot::HttpError;
use dropshot::HttpResponseOk;
use dropshot::HttpResponseUpdatedNoContent;
use dropshot::Path;
use dropshot::RequestContext;
use dropshot::UntypedBody;
use gateway_client::types::SpIdentifier;
use gateway_client::types::SpType;
use omicron_common::api::internal::nexus::UpdateArtifactId;
use schemars::JsonSchema;
use serde::Serialize;
use std::collections::BTreeMap;

use crate::ServerContext;

type WicketdApiDescription = ApiDescription<ServerContext>;

/// Return a description of the wicketd api for use in generating an OpenAPI spec
pub fn api() -> WicketdApiDescription {
    fn register_endpoints(
        api: &mut WicketdApiDescription,
    ) -> Result<(), String> {
        api.register(get_inventory)?;
        api.register(put_repository)?;
        api.register(get_artifacts)?;
        api.register(post_start_update)?;
        api.register(get_update_all)?;
        api.register(get_update_sp)?;
        api.register(post_reset_sp)?;
        Ok(())
    }

    let mut api = WicketdApiDescription::new();
    if let Err(err) = register_endpoints(&mut api) {
        panic!("failed to register entrypoints: {}", err);
    }
    api
}

/// A status endpoint used to report high level information known to wicketd.
///
/// This endpoint can be polled to see if there have been state changes in the
/// system that are useful to report to wicket.
///
/// Wicket, and possibly other callers, will retrieve the changed information,
/// with follow up calls.
#[endpoint {
    method = GET,
    path = "/inventory"
}]
async fn get_inventory(
    rqctx: RequestContext<ServerContext>,
) -> Result<HttpResponseOk<GetInventoryResponse>, HttpError> {
    match rqctx.context().mgs_handle.get_inventory().await {
        Ok(response) => Ok(HttpResponseOk(response)),
        Err(_) => {
            Err(HttpError::for_unavail(None, "Server is shutting down".into()))
        }
    }
}

/// An endpoint used to upload TUF repositories to the server.
#[endpoint {
    method = PUT,
    path = "/repositories/{name}/{version}",
}]
async fn put_repository(
    rqctx: RequestContext<ServerContext>,
    path: Path<TufRepositoryId>,
    body: UntypedBody,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    // TODO: do we need to return more information with the response?

    // TODO: `UntypedBody` is currently inefficient for large request bodies -- it does many copies
    // and allocations. Replace this with a better solution once it's available in dropshot.
    rqctx
        .context()
        .artifact_store
        .add_repository(path.into_inner(), body.as_bytes())?;
    Ok(HttpResponseUpdatedNoContent())
}

/// The response to a `get_artifacts` call: the list of all artifacts currently
/// held by wicketd.
#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct GetArtifactsResponse {
    pub artifacts: Vec<UpdateArtifactId>,
}

/// An endpoint used to report all available artifacts.
///
/// The order of the returned artifacts is unspecified, and may change between
/// calls even if the total set of artifacts has not.
#[endpoint {
    method = GET,
    path = "/artifacts",
}]
async fn get_artifacts(
    rqctx: RequestContext<ServerContext>,
) -> Result<HttpResponseOk<GetArtifactsResponse>, HttpError> {
    let artifacts = rqctx.context().artifact_store.artifact_ids();
    Ok(HttpResponseOk(GetArtifactsResponse { artifacts }))
}

/// An endpoint to start updating a sled.
#[endpoint {
    method = POST,
    path = "/update/{type}/{slot}",
}]
async fn post_start_update(
    rqctx: RequestContext<ServerContext>,
    target: Path<SpIdentifier>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    match rqctx.context().update_planner.start(target.into_inner()).await {
        Ok(()) => Ok(HttpResponseUpdatedNoContent {}),
        Err(err) => match err {
            UpdatePlanError::DuplicateArtifacts(_)
            | UpdatePlanError::MissingArtifact(_) => {
                // TODO-correctness for_bad_request may not be right - both of
                // these errors are issues with the TUF repository, not this
                // request itself.
                Err(HttpError::for_bad_request(None, err.to_string()))
            }
            UpdatePlanError::UpdateInProgress(_) => {
                Err(HttpError::for_unavail(None, err.to_string()))
            }
        },
    }
}

/// The response to a `get_update_all` call: the list of all updates (in-flight
/// or completed) known by wicketd.
#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct UpdateLogAll {
    pub sps: BTreeMap<SpType, BTreeMap<u32, UpdateLog>>,
}

/// An endpoint to get the status of all updates being performed or recently
/// completed on all SPs.
#[endpoint {
    method = GET,
    path = "/update",
}]
async fn get_update_all(
    rqctx: RequestContext<ServerContext>,
) -> Result<HttpResponseOk<UpdateLogAll>, HttpError> {
    let sps = rqctx.context().update_planner.update_log_all().await;
    Ok(HttpResponseOk(UpdateLogAll { sps }))
}

/// An endpoint to get the status of any update being performed or recently
/// completed on a single SP.
#[endpoint {
    method = GET,
    path = "/update/{type}/{slot}",
}]
async fn get_update_sp(
    rqctx: RequestContext<ServerContext>,
    target: Path<SpIdentifier>,
) -> Result<HttpResponseOk<UpdateLog>, HttpError> {
    let update_log =
        rqctx.context().update_planner.update_log(target.into_inner()).await;
    Ok(HttpResponseOk(update_log))
}

/// An endpoint to reset an SP.
#[endpoint {
    method = POST,
    path = "/reset/sp/{type}/{slot}",
}]
async fn post_reset_sp(
    rqctx: RequestContext<ServerContext>,
    target: Path<SpIdentifier>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let sp = target.into_inner();
    rqctx
        .context()
        .mgs_client
        .sp_reset(sp.type_, sp.slot)
        .await
        .map_err(map_mgs_client_error)?;

    Ok(HttpResponseUpdatedNoContent {})
}

fn map_mgs_client_error(
    err: gateway_client::Error<gateway_client::types::Error>,
) -> HttpError {
    use gateway_client::Error;

    match err {
        Error::InvalidRequest(message) => {
            HttpError::for_bad_request(None, message)
        }
        Error::CommunicationError(err) | Error::InvalidResponsePayload(err) => {
            HttpError::for_internal_error(err.to_string())
        }
        Error::UnexpectedResponse(response) => HttpError::for_internal_error(
            format!("unexpected response from MGS: {:?}", response.status()),
        ),
        // Proxy MGS's response to our caller.
        Error::ErrorResponse(response) => {
            let status_code = response.status();
            let response = response.into_inner();
            HttpError {
                status_code,
                error_code: response.error_code,
                external_message: response.message,
                internal_message: format!(
                    "error response from MGS (request_id = {})",
                    response.request_id
                ),
            }
        }
    }
}
