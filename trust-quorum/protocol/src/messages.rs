// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Messsages for the trust quorum protocol

#[cfg(feature = "testing")]
use crate::configuration::configurations_equal_except_for_crypto_data;
use crate::crypto::LrtqShare;
use crate::{Configuration, Epoch};
use gfss::shamir::Share;
use omicron_uuid_kinds::RackUuid;
use serde::{Deserialize, Serialize};

// Re-export message types from trust-quorum-types for backward compatibility.
// These types were previously defined here but have been moved to support
// API versioning per RFD 619.
pub use trust_quorum_types::messages::{
    CommitRequest, LrtqUpgradeMsg, PrepareAndCommitRequest, ReconfigureMsg,
};

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
            ) => configurations_equal_except_for_crypto_data(config1, config2),
            (
                Self::Share { epoch: epoch1, .. },
                Self::Share { epoch: epoch2, .. },
            ) => epoch1 == epoch2,
            (Self::LrtqShare(_), Self::LrtqShare(_)) => true,
            (Self::CommitAdvance(config1), Self::CommitAdvance(config2)) => {
                configurations_equal_except_for_crypto_data(config1, config2)
            }
            (s, o) => s == o,
        }
    }
}
