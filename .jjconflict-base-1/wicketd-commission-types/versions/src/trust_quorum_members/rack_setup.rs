// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// TODO-RAINCLAUDE: module doc: rack setup types for the `TRUST_QUORUM_MEMBERS` version.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use trust_quorum_types_versions::v2::membership::{
    TrustQuorumMembers, TrustQuorumMembersError,
};

use crate::v5;
use crate::v5::rack_setup::RackNetworkConfig;

/// A request to join this rack into an existing multirack cluster.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct MultirackJoinRequest {
    /// The peers required to initialize this rack's trust quorum.
    ///
    /// Unlike RSS, this is not optional: the bootstrap agent discovers
    /// bootstrap addresses and maps them to these `BaseboardId`s.
    pub trust_quorum_peers: TrustQuorumMembers,

    /// The network configuration for this joining rack.
    pub rack_network_config: RackNetworkConfig,
}

impl TryFrom<v5::rack_setup::MultirackJoinRequest> for MultirackJoinRequest {
    type Error = TrustQuorumMembersError;

    fn try_from(
        old: v5::rack_setup::MultirackJoinRequest,
    ) -> Result<Self, Self::Error> {
        let v5::rack_setup::MultirackJoinRequest {
            trust_quorum_peers,
            rack_network_config,
        } = old;
        Ok(Self {
            trust_quorum_peers: TrustQuorumMembers::new(trust_quorum_peers)?,
            rack_network_config,
        })
    }
}
