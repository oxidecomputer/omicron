// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Messsages for the trust quorum protocol

use crate::crypto::LrtqShare;
use crate::{Configuration, Epoch, PlatformId, Threshold};
use gfss::shamir::Share;
use omicron_uuid_kinds::RackUuid;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// A request from nexus informing a node to start coordinating a
/// reconfiguration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconfigureMsg {
    pub rack_id: RackUuid,
    pub epoch: Epoch,
    pub last_committed_epoch: Option<Epoch>,
    pub members: BTreeSet<PlatformId>,
    pub threshold: Threshold,
}

/// A request from nexus informing a node to start coordinating an upgrade from
/// LRTQ
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LrtqUpgradeMsg {
    pub rack_id: RackUuid,
    pub epoch: Epoch,
    // The members of the LRTQ cluster must be the same as the members of the
    // upgraded trust quorum cluster. This is implicit, as the membership of the
    // LRTQ cluster is computed based on the existing control plane sleds known
    // to Nexus.
    pub members: BTreeSet<PlatformId>,
    pub threshold: Threshold,
}

/// Messages sent between trust quorum members over a sprockets channel
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "danger_partial_eq_ct_wrapper", derive(PartialEq, Eq))]
pub struct PeerMsg {
    pub rack_id: RackUuid,
    pub kind: PeerMsgKind,
}

impl PeerMsg {
    #[cfg(feature = "testing")]
    pub fn equal_except_for_crypto_data(&self, other: &Self) -> bool {
        self.rack_id == other.rack_id
            && self.kind.equal_except_for_crypto_data(&other.kind)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "danger_partial_eq_ct_wrapper", derive(PartialEq, Eq))]
pub enum PeerMsgKind {
    /// Sent from a coordinator node to inform a peer about a new configuration
    Prepare {
        config: Configuration,
        share: Share,
    },

    /// Acknowledge a successful prepare from a coordinator
    PrepareAck(Epoch),

    /// Request a node's key share for the given epoch from that node
    GetShare(Epoch),

    /// Return a node's key share in response to a `GetShare` message
    Share {
        epoch: Epoch,
        share: Share,
    },

    // LRTQ shares are always at epoch 1
    GetLrtqShare,

    LrtqShare(LrtqShare),

    /// Inform a node that it is no longer part of the trust quorum as of the
    /// given epoch, which the responder knows is commmitted.
    Expunged(Epoch),

    /// Inform a node that it is utilizing an old committed onfiguration and
    /// give it the new configuration.
    ///
    /// As a result, a requesting node may have to retrieve key shares to
    /// recompute its share if it never received a prepare message for this
    /// epoch.
    CommitAdvance(Configuration),
}

impl PeerMsgKind {
    pub fn name(&self) -> &str {
        match self {
            Self::Prepare { .. } => "prepare",
            Self::PrepareAck(_) => "prepare_ack",
            Self::GetShare(_) => "get_share",
            Self::Share { .. } => "share",
            Self::GetLrtqShare => "get_lrtq_share",
            Self::LrtqShare(_) => "lrtq_share",
            Self::Expunged(_) => "expunged",
            Self::CommitAdvance(_) => "commit_advance",
        }
    }

    /// This is useful for our replay tests without having to worry about seeding
    /// the various random number generators in our production code.
    #[cfg(feature = "testing")]
    pub fn equal_except_for_crypto_data(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Prepare { config: config1, .. },
                Self::Prepare { config: config2, .. },
            ) => config1.equal_except_for_crypto_data(config2),
            (
                Self::Share { epoch: epoch1, .. },
                Self::Share { epoch: epoch2, .. },
            ) => epoch1 == epoch2,
            (Self::LrtqShare(_), Self::LrtqShare(_)) => true,
            (Self::CommitAdvance(config1), Self::CommitAdvance(config2)) => {
                config1.equal_except_for_crypto_data(config2)
            }
            (s, o) => s == o,
        }
    }
}
