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
use dropshot::TypedBody;
use dropshot::UntypedBody;
use gateway_client::types::SpIdentifier;
use gateway_client::types::SpType;
use omicron_common::api::internal::nexus::UpdateArtifactId;
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
        api.register(get_component_update_status)?;
        api.register(post_component_update_abort)?;
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
    let inventory = rqctx.context().mgs_handle.get_inventory().await?;
    Ok(HttpResponseOk(inventory))
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

/// Description of a specific component on a target SP.
#[derive(Clone, Debug, JsonSchema, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SpComponentIdentifier {
    #[serde(rename = "type")]
    pub type_: SpType,
    pub slot: u32,
    pub component: String, // TODO should this be UpdateArtifactKind?
}

/// An endpoint to request the current status of an update being applied to a
/// component by MGS.
#[endpoint {
    method = GET,
    path = "/update/{type}/{slot}/{component}",
}]
async fn get_component_update_status(
    rqctx: RequestContext<ServerContext>,
    target: Path<SpComponentIdentifier>,
) -> Result<HttpResponseOk<gateway_client::types::SpUpdateStatus>, HttpError> {
    let response = rqctx
        .context()
        .mgs_handle
        .get_component_update_status(target.into_inner())
        .await?;

    Ok(HttpResponseOk(response))
}

/// An endpoint to abort an in-progress, failed, or stalled update being applied
/// to a component by MGS.
#[endpoint {
    method = POST,
    path = "/update/{type}/{slot}/{component}/abort",
}]
async fn post_component_update_abort(
    rqctx: RequestContext<ServerContext>,
    target: Path<SpComponentIdentifier>,
    body: TypedBody<Uuid>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    rqctx
        .context()
        .mgs_handle
        .component_update_abort(target.into_inner(), body.into_inner())
        .await?;

    Ok(HttpResponseUpdatedNoContent {})
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
    rqctx.context().mgs_handle.sp_reset(target.into_inner()).await?;

    Ok(HttpResponseUpdatedNoContent {})
}
