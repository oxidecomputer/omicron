// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! HTTP entrypoint functions for wicketd

use crate::mgs::GetInventoryError;
use crate::mgs::GetInventoryResponse;
use dropshot::endpoint;
use dropshot::ApiDescription;
use dropshot::HttpError;
use dropshot::HttpResponseOk;
use dropshot::HttpResponseUpdatedNoContent;
use dropshot::Path;
use dropshot::RequestContext;
use dropshot::StreamingBody;
use dropshot::TypedBody;
use futures::TryStreamExt;
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
use wicket_common::update_events::EventReport;

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
        Err(GetInventoryError::InvalidSpIdentifier) => {
            Err(HttpError::for_unavail(
                None,
                "Invalid SP identifier in request".into(),
            ))
        }
        Err(GetInventoryError::ShutdownInProgress) => {
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
    body: StreamingBody,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let rqctx = rqctx.context();

    // TODO: do we need to return more information with the response?

    let bytes = body.into_stream().try_collect().await?;
    rqctx.update_tracker.put_repository(bytes).await?;

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
        rqctx.context().update_tracker.system_version_and_artifact_ids().await;
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
    let target = target.into_inner();

    // Can we update the target SP? We refuse to update if:
    //
    // 1. We haven't pulled its state in our inventory (most likely cause: the
    //    cubby is empty; less likely cause: the SP is misbehaving, which will
    //    make updating it very unlikely to work anyway)
    // 2. We have pulled its state but our hardware manager says we can't update
    //    it (most likely cause: the target is the sled we're currently running
    //    on; less likely cause: our hardware manager failed to get our local
    //    identifying information, and it refuses to update this target out of
    //    an abundance of caution).
    //
    // First, get our most-recently-cached inventory view.
    let inventory = match rqctx.mgs_handle.get_inventory(Vec::new()).await {
        Ok(inventory) => inventory,
        Err(GetInventoryError::ShutdownInProgress) => {
            return Err(HttpError::for_unavail(
                None,
                "Server is shutting down".into(),
            ));
        }
        // We didn't specify any SP ids to refresh, so this error is impossible.
        Err(GetInventoryError::InvalidSpIdentifier) => unreachable!(),
    };

    // Next, do we have the state of the target SP?
    let sp_state = match inventory {
        GetInventoryResponse::Response { inventory, .. } => inventory
            .sps
            .into_iter()
            .filter_map(|sp| if sp.id == target { sp.state } else { None })
            .next(),
        GetInventoryResponse::Unavailable => None,
    };
    let Some(sp_state) = sp_state else {
        return Err(HttpError::for_bad_request(
            None,
            "cannot update target sled (no inventory state present)"
                .into(),
        ));
    };

    // If we have the state of the SP, are we allowed to update it? We
    // refuse to try to update our own sled.
    match &rqctx.baseboard {
        Some(baseboard) => {
            if baseboard.identifier() == sp_state.serial_number
                && baseboard.model() == sp_state.model
                && baseboard.revision() == i64::from(sp_state.revision)
            {
                return Err(HttpError::for_bad_request(
                    None,
                    "cannot update sled where wicketd is running".into(),
                ));
            }
        }
        None => {
            // We don't know our own baseboard, which is a very
            // questionable state to be in! For now, we will hard-code
            // the possibly locations where we could be running:
            // scrimlets can only be in cubbies 14 or 16, so we refuse
            // to update either of those.
            let target_is_scrimlet =
                matches!((target.type_, target.slot), (SpType::Sled, 14 | 16));
            if target_is_scrimlet {
                return Err(HttpError::for_bad_request(
                    None,
                    "wicketd does not know its own baseboard details: \
                     refusing to update either scrimlet"
                        .into(),
                ));
            }
        }
    }

    // All pre-flight update checks look OK: start the update.
    //
    // Generate an ID for this update; the update tracker will send it to the
    // sled as part of the InstallinatorImageId, and installinator will send it
    // back to our artifact server with its progress reports.
    let update_id = Uuid::new_v4();

    match rqctx.update_tracker.start(target, update_id).await {
        Ok(()) => Ok(HttpResponseUpdatedNoContent {}),
        Err(err) => Err(err.to_http_error()),
    }
}

/// The response to a `get_update_all` call: the list of all updates (in-flight
/// or completed) known by wicketd.
#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct EventReportAll {
    pub sps: BTreeMap<SpType, BTreeMap<u32, EventReport>>,
}

/// An endpoint to get the status of all updates being performed or recently
/// completed on all SPs.
#[endpoint {
    method = GET,
    path = "/update",
}]
async fn get_update_all(
    rqctx: RequestContext<ServerContext>,
) -> Result<HttpResponseOk<EventReportAll>, HttpError> {
    let sps = rqctx.context().update_tracker.event_report_all().await;
    Ok(HttpResponseOk(EventReportAll { sps }))
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
) -> Result<HttpResponseOk<EventReport>, HttpError> {
    let event_report =
        rqctx.context().update_tracker.event_report(target.into_inner()).await;
    Ok(HttpResponseOk(event_report))
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
