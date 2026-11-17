// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use trust_quorum_types_versions::v1::status::CoordinatorStatus;
use trust_quorum_types_versions::v2::status::NotCoordinatingStatus;

use crate::v61;

/// Response type for `trust_quorum_coordinator_status`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum TrustQuorumCoordinatorStatusResponse {
    /// The node is currently coordinating a reconfiguration or LRTQ upgrade.
    Coordinating {
        /// The coordinator status.
        status: CoordinatorStatus,
    },
    /// The node is not currently coordinating a reconfiguration or LRTQ
    /// upgrade.
    NotCoordinating(NotCoordinatingStatus),
}

impl From<TrustQuorumCoordinatorStatusResponse>
    for v61::trust_quorum::TrustQuorumCoordinatorStatusResponse
{
    fn from(value: TrustQuorumCoordinatorStatusResponse) -> Self {
        match value {
            TrustQuorumCoordinatorStatusResponse::Coordinating { status } => {
                Self::Coordinating { status }
            }
            TrustQuorumCoordinatorStatusResponse::NotCoordinating(
                NotCoordinatingStatus {
                    latest_config_epoch,
                    latest_committed_epoch: _,
                },
            ) => Self::NotCoordinating { latest_config_epoch },
        }
    }
}
