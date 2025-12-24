// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Status report types for trust quorum nodes.

use std::collections::BTreeSet;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::alarm::Alarm;
use super::configuration::{BaseboardId, Configuration};
use super::persistent_state::ExpungedMetadata;
use super::types::Epoch;

/// Whether or not a configuration has been committed or is still underway.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CommitStatus {
    Committed,
    Pending,
}

/// Status of the node coordinating the reconfiguration or LRTQ upgrade.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CoordinatorStatus {
    pub config: Configuration,
    pub acked_prepares: BTreeSet<BaseboardId>,
}

/// Details about a given node's status.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct NodeStatus {
    pub connected_peers: BTreeSet<BaseboardId>,
    pub alarms: BTreeSet<Alarm>,
    pub persistent_state: NodePersistentStateSummary,
    pub proxied_requests: u64,
}

/// A summary of a node's persistent state.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct NodePersistentStateSummary {
    pub has_lrtq_share: bool,
    pub configs: BTreeSet<Epoch>,
    pub shares: BTreeSet<Epoch>,
    pub commits: BTreeSet<Epoch>,
    pub expunged: Option<ExpungedMetadata>,
}
