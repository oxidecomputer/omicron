// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cockroach_cli::NodeDecommission;
use crate::cockroach_cli::NodeStatus;
use crate::context::ServerContext;
use dropshot::endpoint;
use dropshot::ApiDescriptionRegisterError;
use dropshot::HttpError;
use dropshot::HttpResponseOk;
use dropshot::RequestContext;
use dropshot::TypedBody;
use omicron_uuid_kinds::OmicronZoneUuid;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;

type CrdbApiDescription = dropshot::ApiDescription<Arc<ServerContext>>;

pub fn api() -> CrdbApiDescription {
    fn register_endpoints(
        api: &mut CrdbApiDescription,
    ) -> Result<(), ApiDescriptionRegisterError> {
        api.register(local_node_id)?;
        api.register(node_status)?;
        api.register(node_decommission)?;
        Ok(())
    }

    let mut api = CrdbApiDescription::new();
    if let Err(err) = register_endpoints(&mut api) {
        panic!("failed to register entrypoints: {}", err);
    }
    api
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct ClusterNodeStatus {
    pub all_nodes: Vec<NodeStatus>,
}

/// Get the status of all nodes in the CRDB cluster
#[endpoint {
    method = GET,
    path = "/node/status",
}]
async fn node_status(
    rqctx: RequestContext<Arc<ServerContext>>,
) -> Result<HttpResponseOk<ClusterNodeStatus>, HttpError> {
    let ctx = rqctx.context();
    let all_nodes =
        ctx.cockroach_cli().node_status().await.map_err(HttpError::from)?;
    Ok(HttpResponseOk(ClusterNodeStatus { all_nodes }))
}

/// CockroachDB Node ID
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct LocalNodeId {
    /// The ID of this Omicron zone.
    ///
    /// This is included to ensure correctness even if a socket address on a
    /// sled is reused for a different zone; if our caller is trying to
    /// determine the node ID for a particular Omicron CockroachDB zone, they'll
    /// contact us by socket address. We include our zone ID in the response for
    /// their confirmation that we are the zone they intended to contact.
    pub zone_id: OmicronZoneUuid,
    // CockroachDB node IDs are integers, in practice, but our use of them is as
    // input and output to the `cockroach` CLI. We use a string which is a bit
    // more natural (no need to parse CLI output or stringify an ID to send it
    // as input) and leaves open the door for the format to change in the
    // future.
    pub node_id: String,
}

/// Get the CockroachDB node ID of the local cockroach instance.
#[endpoint {
    method = GET,
    path = "/node/id",
}]
async fn local_node_id(
    rqctx: RequestContext<Arc<ServerContext>>,
) -> Result<HttpResponseOk<LocalNodeId>, HttpError> {
    let ctx = rqctx.context();
    let node_id = ctx.node_id().await?.to_string();
    let zone_id = ctx.zone_id();
    Ok(HttpResponseOk(LocalNodeId { zone_id, node_id }))
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct NodeId {
    pub node_id: String,
}

/// Decommission a node from the CRDB cluster
#[endpoint {
    method = POST,
    path = "/node/decommission",
}]
async fn node_decommission(
    rqctx: RequestContext<Arc<ServerContext>>,
    body: TypedBody<NodeId>,
) -> Result<HttpResponseOk<NodeDecommission>, HttpError> {
    let ctx = rqctx.context();
    let NodeId { node_id } = body.into_inner();
    let decommission_status =
        ctx.cockroach_cli().node_decommission(&node_id).await?;
    Ok(HttpResponseOk(decommission_status))
}
