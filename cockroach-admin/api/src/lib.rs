// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use cockroach_admin_types::{NodeDecommission, NodeStatus};
use dropshot::{
    HttpError, HttpResponseOk, HttpResponseUpdatedNoContent, RequestContext,
    TypedBody,
};
use omicron_uuid_kinds::OmicronZoneUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[dropshot::api_description]
pub trait CockroachAdminApi {
    type Context;

    /// Initialize the CockroachDB cluster.
    ///
    /// This performs both the base-level `cockroach init` and installs the
    /// Omicron schema. It should be idempotent, but we haven't heavily tested
    /// that. We test that this endpoint can safely be called multiple times,
    /// but haven't tested calling it concurrently (either multiple simultaneous
    /// requests to the same cockroach node, or sending simultaneous requests to
    /// different cockroach nodes, both of which would rely on `cockroach init`
    /// itself being safe to call concurrently). In practice, only RSS calls
    /// this endpoint and it does so serially; as long as we don't change that,
    /// the existing testing should be sufficient.
    #[endpoint {
        method = POST,
        path = "/cluster/init",
    }]
    async fn cluster_init(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

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

    /// Proxy to CockroachDB's /_status/vars endpoint
    //
    // Dropshot isn't happy about the "_status" portion of the path; it fails
    // the linter. Instead, I'm adding the prefix "proxy" to make it clear these
    // are "intended-to-be-proxied" endpoints, rather than exact matches for the
    // CRDB HTTP interface paths.
    #[endpoint {
        method = GET,
        path = "/proxy/status/vars",
    }]
    async fn status_vars(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<String>, HttpError>;

    /// Proxy to CockroachDB's /_status/nodes endpoint
    #[endpoint {
        method = GET,
        path = "/proxy/status/nodes",
    }]
    async fn status_nodes(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<String>, HttpError>;
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
