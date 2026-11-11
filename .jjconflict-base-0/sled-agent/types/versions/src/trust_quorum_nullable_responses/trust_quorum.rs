// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use trust_quorum_types_versions::v1::status::CoordinatorStatus;
use trust_quorum_types_versions::v1::types::Epoch;

use crate::v15::trust_quorum::TrustQuorumNetworkConfig;

/// Response type for `trust_quorum_coordinator_status`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum TrustQuorumCoordinatorStatusResponse {
    /// The node is currently coordinating a reconfiguration or LRTQ upgrade.
    Coordinating {
        /// The coordinator status.
        status: CoordinatorStatus,
    },
    /// The node is not currently coordinating a reconfiguration or LRTQ upgrade.
    NotCoordinating {
        /// The latest configuration epoch this node has persisted.
        ///
        /// If this is greater than or equal to the epoch that Nexus is driving,
        /// the coordinator started that epoch and then stopped for some reason,
        /// and Nexus can abort this configuration. If lower than the Nexus
        /// epoch, then reconfiguration has not started yet.
        latest_config_epoch: Option<Epoch>,
    },
}

/// Response type for `trust_quorum_network_config`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TrustQuorumNetworkConfigResponse {
    /// The trust quorum network config, or `None` if a config has not been
    /// loaded or received since Sled Agent started.
    pub config: Option<TrustQuorumNetworkConfig>,
}
