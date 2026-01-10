// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Trust quorum protocol messages and API request types.

use std::collections::BTreeSet;

use omicron_uuid_kinds::RackUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::configuration::{BaseboardId, Configuration};
use super::types::{Epoch, Threshold};

/// A request from Nexus informing a node to start coordinating a
/// reconfiguration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ReconfigureMsg {
    pub rack_id: RackUuid,
    pub epoch: Epoch,
    pub last_committed_epoch: Option<Epoch>,
    pub members: BTreeSet<BaseboardId>,
    pub threshold: Threshold,
}

/// A request from Nexus informing a node to start coordinating an upgrade from
/// LRTQ.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct LrtqUpgradeMsg {
    pub rack_id: RackUuid,
    pub epoch: Epoch,
    // The members of the LRTQ cluster must be the same as the members of the
    // upgraded trust quorum cluster. This is implicit, as the membership of the
    // LRTQ cluster is computed based on the existing control plane sleds known
    // to Nexus.
    pub members: BTreeSet<BaseboardId>,
    pub threshold: Threshold,
}

/// Request to commit a trust quorum configuration at a given epoch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct CommitRequest {
    pub rack_id: RackUuid,
    pub epoch: Epoch,
}

/// Request to prepare and commit a trust quorum configuration.
///
/// This is the `Configuration` sent to a node that missed the `Prepare` phase.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PrepareAndCommitRequest {
    pub config: Configuration,
}
