// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Node-related types for the CockroachDB Admin API.

use chrono::{DateTime, Utc};
use omicron_uuid_kinds::OmicronZoneUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

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

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct NodeStatus {
    // TODO use NodeId
    pub node_id: String,
    pub address: SocketAddr,
    pub sql_address: SocketAddr,
    pub build: String,
    pub started_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub locality: String,
    pub is_available: bool,
    pub is_live: bool,
    pub replicas_leaders: i64,
    pub replicas_leaseholders: i64,
    pub ranges: i64,
    pub ranges_unavailable: i64,
    pub ranges_underreplicated: i64,
    pub live_bytes: i64,
    pub key_bytes: i64,
    pub value_bytes: i64,
    pub intent_bytes: i64,
    pub system_bytes: i64,
    pub gossiped_replicas: i64,
    pub is_decommissioning: bool,
    pub membership: String,
    pub is_draining: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct NodeDecommission {
    pub node_id: String,
    pub is_live: bool,
    pub replicas: i64,
    pub is_decommissioning: bool,
    pub membership: NodeMembership,
    pub is_draining: bool,
    pub notes: Vec<String>,
}

// The cockroach CLI and `crdb_internal.gossip_liveness` table use a string for
// node membership, but there are only three meaningful values per
// https://github.com/cockroachdb/cockroach/blob/0c92c710d2baadfdc5475be8d2238cf26cb152ca/pkg/kv/kvserver/liveness/livenesspb/liveness.go#L96,
// so we'll convert into a Rust enum and leave the "unknown" case for future
// changes that expand or reword these values.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "state", rename_all = "lowercase")]
pub enum NodeMembership {
    Active,
    Decommissioning,
    Decommissioned,
    Unknown { value: String },
}
