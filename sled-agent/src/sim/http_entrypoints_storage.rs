// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! HTTP entrypoint functions for simulating the storage agent API.

use crucible_agent_client::types::{
    CreateRegion, GetSnapshotResponse, Region, RegionId, RunningSnapshot,
    Snapshot,
};
use dropshot::{
    endpoint, ApiDescription, ApiDescriptionRegisterError, HttpError,
    HttpResponseDeleted, HttpResponseOk, Path as TypedPath, RequestContext,
    TypedBody,
};
use schemars::JsonSchema;
use serde::Deserialize;
use std::sync::Arc;

use super::storage::CrucibleData;

type CrucibleAgentApiDescription = ApiDescription<Arc<CrucibleData>>;

/// Returns a description of the Crucible Agent API.
pub fn api() -> CrucibleAgentApiDescription {
    fn register_endpoints(
        api: &mut CrucibleAgentApiDescription,
    ) -> Result<(), ApiDescriptionRegisterError> {
        api.register(region_list)?;
        api.register(region_create)?;
        api.register(region_get)?;
        api.register(region_delete)?;

        api.register(region_get_snapshots)?;
        api.register(region_get_snapshot)?;
        api.register(region_delete_snapshot)?;
        api.register(region_run_snapshot)?;
        api.register(region_delete_running_snapshot)?;

        Ok(())
    }

    let mut api = CrucibleAgentApiDescription::new();
    if let Err(err) = register_endpoints(&mut api) {
        panic!("failed to register entrypoints: {}", err);
    }
    api
}

// TODO: We'd like to de-duplicate as much as possible with the
// real crucible agent here, to avoid skew.

#[derive(Deserialize, JsonSchema)]
struct RegionPath {
    id: RegionId,
}

#[endpoint {
    method = GET,
    path = "/crucible/0/regions",
}]
async fn region_list(
    rc: RequestContext<Arc<CrucibleData>>,
) -> Result<HttpResponseOk<Vec<Region>>, HttpError> {
    let crucible = rc.context();
    Ok(HttpResponseOk(crucible.list().await))
}

#[endpoint {
    method = POST,
    path = "/crucible/0/regions",
}]
async fn region_create(
    rc: RequestContext<Arc<CrucibleData>>,
    body: TypedBody<CreateRegion>,
) -> Result<HttpResponseOk<Region>, HttpError> {
    let params = body.into_inner();
    let crucible = rc.context();

    let region = crucible.create(params).await.map_err(|e| {
        HttpError::for_internal_error(
            format!("region create failure: {:?}", e,),
        )
    })?;

    Ok(HttpResponseOk(region))
}

#[endpoint {
    method = GET,
    path = "/crucible/0/regions/{id}",
}]
async fn region_get(
    rc: RequestContext<Arc<CrucibleData>>,
    path: TypedPath<RegionPath>,
) -> Result<HttpResponseOk<Region>, HttpError> {
    let id = path.into_inner().id;
    let crucible = rc.context();

    match crucible.get(id).await {
        Some(region) => Ok(HttpResponseOk(region)),
        None => {
            Err(HttpError::for_not_found(None, "Region not found".to_string()))
        }
    }
}

#[endpoint {
    method = DELETE,
    path = "/crucible/0/regions/{id}",
}]
async fn region_delete(
    rc: RequestContext<Arc<CrucibleData>>,
    path: TypedPath<RegionPath>,
) -> Result<HttpResponseDeleted, HttpError> {
    let id = path.into_inner().id;
    let crucible = rc.context();

    crucible
        .delete(id)
        .await
        .map_err(|e| HttpError::for_bad_request(None, e.to_string()))?;

    Ok(HttpResponseDeleted())
}

#[endpoint {
    method = GET,
    path = "/crucible/0/regions/{id}/snapshots",
}]
async fn region_get_snapshots(
    rc: RequestContext<Arc<CrucibleData>>,
    path: TypedPath<RegionPath>,
) -> Result<HttpResponseOk<GetSnapshotResponse>, HttpError> {
    let id = path.into_inner().id;

    let crucible = rc.context();

    if crucible.get(id.clone()).await.is_none() {
        return Err(HttpError::for_not_found(
            None,
            "Region not found".to_string(),
        ));
    }

    let snapshots = crucible.snapshots_for_region(&id).await;

    let running_snapshots = crucible.running_snapshots_for_id(&id).await;

    Ok(HttpResponseOk(GetSnapshotResponse { snapshots, running_snapshots }))
}

#[derive(Deserialize, JsonSchema)]
struct GetSnapshotPath {
    id: RegionId,
    name: String,
}

#[endpoint {
    method = GET,
    path = "/crucible/0/regions/{id}/snapshots/{name}",
}]
async fn region_get_snapshot(
    rc: RequestContext<Arc<CrucibleData>>,
    path: TypedPath<GetSnapshotPath>,
) -> Result<HttpResponseOk<Snapshot>, HttpError> {
    let p = path.into_inner();
    let crucible = rc.context();

    if crucible.get(p.id.clone()).await.is_none() {
        return Err(HttpError::for_not_found(
            None,
            "Region not found".to_string(),
        ));
    }

    for snapshot in &crucible.snapshots_for_region(&p.id).await {
        if snapshot.name == p.name {
            return Ok(HttpResponseOk(snapshot.clone()));
        }
    }

    Err(HttpError::for_not_found(
        None,
        format!("region {:?} snapshot {:?} not found", p.id, p.name),
    ))
}

#[derive(Deserialize, JsonSchema)]
struct DeleteSnapshotPath {
    id: RegionId,
    name: String,
}

#[endpoint {
    method = DELETE,
    path = "/crucible/0/regions/{id}/snapshots/{name}",
}]
async fn region_delete_snapshot(
    rc: RequestContext<Arc<CrucibleData>>,
    path: TypedPath<DeleteSnapshotPath>,
) -> Result<HttpResponseDeleted, HttpError> {
    let p = path.into_inner();
    let crucible = rc.context();

    if crucible.get(p.id.clone()).await.is_none() {
        return Err(HttpError::for_not_found(
            None,
            "Region not found".to_string(),
        ));
    }

    crucible
        .delete_snapshot(&p.id, &p.name)
        .await
        .map_err(|e| HttpError::for_bad_request(None, e.to_string()))?;

    Ok(HttpResponseDeleted())
}

#[derive(Deserialize, JsonSchema)]
struct RunSnapshotPath {
    id: RegionId,
    name: String,
}

#[endpoint {
    method = POST,
    path = "/crucible/0/regions/{id}/snapshots/{name}/run",
}]
async fn region_run_snapshot(
    rc: RequestContext<Arc<CrucibleData>>,
    path: TypedPath<RunSnapshotPath>,
) -> Result<HttpResponseOk<RunningSnapshot>, HttpError> {
    let p = path.into_inner();
    let crucible = rc.context();

    if crucible.get(p.id.clone()).await.is_none() {
        return Err(HttpError::for_not_found(
            None,
            "Region not found".to_string(),
        ));
    }

    let snapshots = crucible.snapshots_for_region(&p.id).await;

    if !snapshots.iter().any(|x| x.name == p.name) {
        return Err(HttpError::for_not_found(
            None,
            format!("snapshot {:?} not found", p.name),
        ));
    }

    let running_snapshot = crucible
        .create_running_snapshot(&p.id, &p.name)
        .await
        .map_err(|e| {
            HttpError::for_internal_error(format!(
                "running snapshot create failure: {:?}",
                e,
            ))
        })?;

    Ok(HttpResponseOk(running_snapshot))
}

#[endpoint {
    method = DELETE,
    path = "/crucible/0/regions/{id}/snapshots/{name}/run",
}]
async fn region_delete_running_snapshot(
    rc: RequestContext<Arc<CrucibleData>>,
    path: TypedPath<RunSnapshotPath>,
) -> Result<HttpResponseDeleted, HttpError> {
    let p = path.into_inner();
    let crucible = rc.context();

    if crucible.get(p.id.clone()).await.is_none() {
        return Err(HttpError::for_not_found(
            None,
            "Region not found".to_string(),
        ));
    }

    crucible.delete_running_snapshot(&p.id, &p.name).await.map_err(|e| {
        HttpError::for_internal_error(format!(
            "running snapshot create failure: {:?}",
            e,
        ))
    })?;

    Ok(HttpResponseDeleted())
}
