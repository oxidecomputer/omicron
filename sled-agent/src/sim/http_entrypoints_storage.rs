// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! HTTP entrypoint functions for simulating the storage agent API.

// use crucible_agent_client::types::{CreateRegion, RegionId};
use dropshot::{
    endpoint, ApiDescription, HttpError, HttpResponseDeleted, HttpResponseOk,
    Path as TypedPath, RequestContext, TypedBody,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use super::storage::CrucibleData;

type CrucibleAgentApiDescription = ApiDescription<Arc<CrucibleData>>;

/**
 * Returns a description of the sled agent API
 */
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

// XXX XXX XXX THIS SUCKS XXX XXX XXX
//
// I need to re-define all structs used in the crucible agent
// API to ensure they have the traits I need. The ones re-exported
// through the client bindings, i.e., crucible_agent_client::types,
// don't implement what I need.
//
// I'd like them to! If we could ensure the generated client
// also implemented e.g. JsonSchema, this might work?
//
// TODO: Try w/RegionId or State first?

#[derive(
    Serialize,
    Deserialize,
    JsonSchema,
    Debug,
    PartialEq,
    Eq,
    Clone,
    PartialOrd,
    Ord,
)]
pub struct RegionId(pub String);

#[derive(Serialize, Deserialize, JsonSchema, Debug, PartialEq, Clone)]
#[serde(rename_all = "lowercase")]
pub enum State {
    Requested,
    Created,
    Tombstoned,
    Destroyed,
    Failed,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, PartialEq, Clone)]
pub struct CreateRegion {
    pub id: RegionId,
    pub volume_id: String,

    pub block_size: u64,
    pub extent_size: u64,
    pub extent_count: u64,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, PartialEq, Clone)]
pub struct Region {
    pub id: RegionId,
    pub volume_id: String,

    pub block_size: u64,
    pub extent_size: u64,
    pub extent_count: u64,

    pub port_number: u16,
    pub state: State,
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

#[derive(Deserialize, JsonSchema)]
struct RegionPath {
    id: RegionId,
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
