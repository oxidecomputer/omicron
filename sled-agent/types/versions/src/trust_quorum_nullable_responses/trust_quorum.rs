// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use trust_quorum_types_versions::v1::status::CoordinatorStatus;

use crate::v15::trust_quorum::TrustQuorumNetworkConfig;

/// Response type for `trust_quorum_coordinator_status`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TrustQuorumCoordinatorStatusResponse {
    /// The coordinator status, or `None` if this node is not currently
    /// coordinating a reconfiguration.
    pub status: Option<CoordinatorStatus>,
}

/// Response type for `trust_quorum_network_config`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TrustQuorumNetworkConfigResponse {
    /// The trust quorum network config, or `None` if a config has not been
    /// loaded or received since Sled Agent started.
    pub config: Option<TrustQuorumNetworkConfig>,
}
