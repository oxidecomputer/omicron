// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use trust_quorum_types_versions::v1::status::CoordinatorStatus;
use trust_quorum_types_versions::v1::types::Epoch;

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
    NotCoordinating {
        /// The latest configuration epoch this node has persisted.
        ///
        /// * If lower than the Nexus epoch, then this node has not started
        ///   coordinating that epoch: it either never received the
        ///   configuration or restarted before persisting it.
        ///
        /// * If equal to the Nexus epoch, then this node started coordinating
        ///   that epoch but is no longer doing so: either it committed the
        ///   epoch (see `latest_committed_epoch`), or it lost its coordination
        ///   state, for example by restarting. Only the latter warrants an
        ///   abort.
        ///
        /// * If greater than the Nexus epoch, then this node has moved on to a
        ///   newer configuration, and the Nexus epoch has been superseded.
        ///
        /// This is `None` if the node has not persisted a trust quorum
        /// configuration, including a node that hasn't yet upgraded from LRTQ.
        latest_config_epoch: Option<Epoch>,
        /// The latest configuration epoch this node has committed.
        ///
        /// This is never greater than `latest_config_epoch`.
        ///
        /// This should be looked at when `latest_config_epoch` is equal to the
        /// Nexus epoch. In that case:
        ///
        /// * If this field is lower than `latest_config_epoch` (including
        ///   `None`), then this node persisted the configuration but stopped
        ///   coordinating it without committing -- in this case, the
        ///   configuration should be aborted (but see below).
        ///
        /// * If this field is equal to `latest_config_epoch`, then this node
        ///   committed the configuration -- in this case, the configuration
        ///   must not be aborted, and Nexus should proceed with committing
        ///   the configuration at the remaining members.
        ///
        /// If this value is lower than the Nexus epoch, it means that _this_
        /// node has not committed that epoch, not that _no_ node has. (For
        /// example, the coordinator may have crashed after Nexus started
        /// committing, but before its own commit arrived.) Aborting must
        /// therefore remain conditional on the configuration not having moved
        /// past the `Preparing` state.
        ///
        /// This is `None` if the node has not committed a trust quorum
        /// configuration, including a node that has not yet upgraded from LRTQ.
        latest_committed_epoch: Option<Epoch>,
    },
}

impl From<TrustQuorumCoordinatorStatusResponse>
    for v61::trust_quorum::TrustQuorumCoordinatorStatusResponse
{
    fn from(value: TrustQuorumCoordinatorStatusResponse) -> Self {
        match value {
            TrustQuorumCoordinatorStatusResponse::Coordinating { status } => {
                Self::Coordinating { status }
            }
            TrustQuorumCoordinatorStatusResponse::NotCoordinating {
                latest_config_epoch,
                latest_committed_epoch: _,
            } => Self::NotCoordinating { latest_config_epoch },
        }
    }
}
