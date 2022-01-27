// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! HTTP entrypoint functions for simulating the storage agent API.

use crucible_agent_client::types::{CreateRegion, Region, RegionId};
use dropshot::{
    endpoint, ApiDescription, HttpError, HttpResponseDeleted, HttpResponseOk,
    Path as TypedPath, RequestContext, TypedBody,
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
    ) -> Result<(), String> {
        api.register(region_list)?;
        api.register(region_create)?;
        api.register(region_get)?;
        api.register(region_delete)?;
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
    rc: Arc<RequestContext<Arc<CrucibleData>>>,
) -> Result<HttpResponseOk<Vec<Region>>, HttpError> {
    let crucible = rc.context();
    Ok(HttpResponseOk(crucible.list().await))
}

#[endpoint {
    method = POST,
    path = "/crucible/0/regions",
}]
async fn region_create(
    rc: Arc<RequestContext<Arc<CrucibleData>>>,
    body: TypedBody<CreateRegion>,
) -> Result<HttpResponseOk<Region>, HttpError> {
    let params = body.into_inner();
    let crucible = rc.context();

    Ok(HttpResponseOk(crucible.create(params).await))
}

#[endpoint {
    method = GET,
    path = "/crucible/0/regions/{id}",
}]
async fn region_get(
    rc: Arc<RequestContext<Arc<CrucibleData>>>,
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
    rc: Arc<RequestContext<Arc<CrucibleData>>>,
    path: TypedPath<RegionPath>,
) -> Result<HttpResponseDeleted, HttpError> {
    let id = path.into_inner().id;
    let crucible = rc.context();

    match crucible.delete(id).await {
        Some(_) => Ok(HttpResponseDeleted()),
        None => {
            Err(HttpError::for_not_found(None, "Region not found".to_string()))
        }
    }
}
