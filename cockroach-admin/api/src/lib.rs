// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use cockroach_admin_types::{NodeDecommission, NodeStatus};
use dropshot::{HttpError, HttpResponseOk, RequestContext, TypedBody};
use omicron_uuid_kinds::OmicronZoneUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[dropshot::api_description {
    module = "cockroach_admin_api_mod",
}]
pub trait CockroachAdminApi {
    type Context;

    /// Get the status of all nodes in the CRDB cluster.
    #[endpoint {
        method = GET,
        path = "/node/status",
    }]
    async fn node_status(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<ClusterNodeStatus>, HttpError>;

    /// Get the CockroachDB node ID of the local cockroach instance.
    #[endpoint {
        method = GET,
        path = "/node/id",
    }]
    async fn local_node_id(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<LocalNodeId>, HttpError>;

    /// Decommission a node from the CRDB cluster.
    #[endpoint {
        method = POST,
        path = "/node/decommission",
    }]
    async fn node_decommission(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<NodeId>,
    ) -> Result<HttpResponseOk<NodeDecommission>, HttpError>;
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct ClusterNodeStatus {
    pub all_nodes: Vec<NodeStatus>,
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

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct NodeId {
    pub node_id: String,
}
