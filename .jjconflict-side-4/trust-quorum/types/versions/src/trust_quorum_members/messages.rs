// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Trust quorum protocol messages and API request types.

use omicron_uuid_kinds::RackUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::membership::{TrustQuorumMembers, TrustQuorumMembersError};
use crate::v1;
use crate::v1::types::{Epoch, Threshold};

/// A request from Nexus informing a node to start coordinating a
/// reconfiguration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ReconfigureMsg {
    pub rack_id: RackUuid,
    pub epoch: Epoch,
    pub last_committed_epoch: Option<Epoch>,
    pub members: TrustQuorumMembers,
    pub threshold: Threshold,
}

impl TryFrom<v1::messages::ReconfigureMsg> for ReconfigureMsg {
    type Error = TrustQuorumMembersError;

    fn try_from(
        old: v1::messages::ReconfigureMsg,
    ) -> Result<Self, Self::Error> {
        let v1::messages::ReconfigureMsg {
            rack_id,
            epoch,
            last_committed_epoch,
            members,
            threshold,
        } = old;
        Ok(Self {
            rack_id,
            epoch,
            last_committed_epoch,
            members: TrustQuorumMembers::new(members)?,
            threshold,
        })
    }
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
    pub members: TrustQuorumMembers,
    pub threshold: Threshold,
}

impl TryFrom<v1::messages::LrtqUpgradeMsg> for LrtqUpgradeMsg {
    type Error = TrustQuorumMembersError;

    fn try_from(
        old: v1::messages::LrtqUpgradeMsg,
    ) -> Result<Self, Self::Error> {
        let v1::messages::LrtqUpgradeMsg { rack_id, epoch, members, threshold } =
            old;
        Ok(Self {
            rack_id,
            epoch,
            members: TrustQuorumMembers::new(members)?,
            threshold,
        })
    }
}
