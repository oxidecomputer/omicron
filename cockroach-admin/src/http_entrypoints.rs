// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cockroach_cli::NodeStatus;
use crate::context::ServerContext;
use dropshot::endpoint;
use dropshot::HttpError;
use dropshot::HttpResponseOk;
use dropshot::RequestContext;
use omicron_uuid_kinds::OmicronZoneUuid;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;

type CrdbApiDescription = dropshot::ApiDescription<Arc<ServerContext>>;

pub fn api() -> CrdbApiDescription {
    fn register_endpoints(api: &mut CrdbApiDescription) -> Result<(), String> {
        api.register(node_id)?;
        api.register(node_status)?;
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
pub struct NodeId {
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
async fn node_id(
    rqctx: RequestContext<Arc<ServerContext>>,
) -> Result<HttpResponseOk<NodeId>, HttpError> {
    let ctx = rqctx.context();
    let node_id = ctx.node_id().await?.to_string();
    let zone_id = ctx.zone_id();
    Ok(HttpResponseOk(NodeId { zone_id, node_id }))
}
