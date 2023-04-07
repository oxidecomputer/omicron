// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! HTTP entrypoint functions for wicketd

use crate::mgs::GetInventoryResponse;
use crate::update_events::UpdateLog;
use dropshot::endpoint;
use dropshot::ApiDescription;
use dropshot::HttpError;
use dropshot::HttpResponseOk;
use dropshot::HttpResponseUpdatedNoContent;
use dropshot::Path;
use dropshot::RequestContext;
use dropshot::TypedBody;
use dropshot::UntypedBody;
use gateway_client::types::IgnitionCommand;
use gateway_client::types::SpIdentifier;
use gateway_client::types::SpType;
use http::StatusCode;
use omicron_common::api::external::SemverVersion;
use omicron_common::update::ArtifactId;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use uuid::Uuid;

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
        api.register(post_ignition_command)?;
        Ok(())
    }

    let mut api = WicketdApiDescription::new();
    if let Err(err) = register_endpoints(&mut api) {
        panic!("failed to register entrypoints: {}", err);
    }
    api
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct GetInventoryParams {
    /// If true, refresh the state of these SPs from MGS prior to returning
    /// (instead of returning cached data).
    pub force_refresh: Vec<SpIdentifier>,
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
    body_params: TypedBody<GetInventoryParams>,
) -> Result<HttpResponseOk<GetInventoryResponse>, HttpError> {
    let GetInventoryParams { force_refresh } = body_params.into_inner();
    match rqctx.context().mgs_handle.get_inventory(force_refresh).await {
        Ok(response) => Ok(HttpResponseOk(response)),
        Err(_) => {
            Err(HttpError::for_unavail(None, "Server is shutting down".into()))
        }
    }
}

/// Upload a TUF repository to the server.
///
/// At any given time, wicketd will keep at most one TUF repository in memory.
/// Any previously-uploaded repositories will be discarded.
#[endpoint {
    method = PUT,
    path = "/repository",
}]
async fn put_repository(
    rqctx: RequestContext<ServerContext>,
    body: UntypedBody,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let rqctx = rqctx.context();

    // TODO: do we need to return more information with the response?

    // TODO: `UntypedBody` is currently inefficient for large request bodies -- it does many copies
    // and allocations. Replace this with a better solution once it's available in dropshot.
    rqctx.artifact_store.put_repository(body.as_bytes())?;

    Ok(HttpResponseUpdatedNoContent())
}

/// The response to a `get_artifacts` call: the system version, and the list of
/// all artifacts currently held by wicketd.
#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct GetArtifactsResponse {
    pub system_version: Option<SemverVersion>,
    pub artifacts: Vec<ArtifactId>,
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
    let (system_version, artifacts) =
        rqctx.context().artifact_store.system_version_and_artifact_ids();
    Ok(HttpResponseOk(GetArtifactsResponse { system_version, artifacts }))
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
    let rqctx = rqctx.context();

    // Do we have a plan with which we can apply updates (i.e., has a valid TUF
    // repository been uploaded)?
    let plan = rqctx.artifact_store.current_plan().ok_or_else(|| {
        // TODO-correctness `for_bad_request` is a little questionable because
        // the problem isn't this request specifically, but that we haven't
        // gotten request yet with a valid TUF repository. `for_unavail` might
        // be more accurate, but `for_unavail` doesn't give us away to give the
        // client a meaningful error.
        HttpError::for_bad_request(
            None,
            "upload a valid TUF repository first".to_string(),
        )
    })?;

    // Generate an ID for this update; the update tracker will send it to the
    // sled as part of the InstallinatorImageId, and installinator will send it
    // back to our artifact server with its progress reports.
    let update_id = Uuid::new_v4();

    match rqctx.update_tracker.start(target.into_inner(), plan, update_id).await
    {
        Ok(()) => Ok(HttpResponseUpdatedNoContent {}),
        Err(err) => Err(err.to_http_error()),
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
    let sps = rqctx.context().update_tracker.update_log_all().await;
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
        rqctx.context().update_tracker.update_log(target.into_inner()).await;
    Ok(HttpResponseOk(update_log))
}

#[derive(Serialize, Deserialize, JsonSchema)]
struct PathSpIgnitionCommand {
    #[serde(rename = "type")]
    type_: SpType,
    slot: u32,
    command: IgnitionCommand,
}

/// Send an ignition command targeting a specific SP.
///
/// This endpoint acts as a proxy to the MGS endpoint performing the same
/// function, allowing wicket to communicate exclusively with wicketd (even
/// though wicketd adds no meaningful functionality here beyond what MGS
/// offers).
#[endpoint {
    method = POST,
    path = "/ignition/{type}/{slot}/{command}",
}]
async fn post_ignition_command(
    rqctx: RequestContext<ServerContext>,
    path: Path<PathSpIgnitionCommand>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = rqctx.context();
    let PathSpIgnitionCommand { type_, slot, command } = path.into_inner();

    apictx
        .mgs_client
        .ignition_command(type_, slot, command)
        .await
        .map_err(http_error_from_client_error)?;

    Ok(HttpResponseUpdatedNoContent())
}

fn http_error_from_client_error(
    err: gateway_client::Error<gateway_client::types::Error>,
) -> HttpError {
    // Most errors have a status code; the only one that definitely doesn't is
    // `Error::InvalidRequest`, for which we'll use `BAD_REQUEST`.
    let status_code = err.status().unwrap_or(StatusCode::BAD_REQUEST);

    let message = format!("request to MGS failed: {err}");

    HttpError {
        status_code,
        error_code: None,
        external_message: message.clone(),
        internal_message: message,
    }
}
