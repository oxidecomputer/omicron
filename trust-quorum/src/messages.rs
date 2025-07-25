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

/// Messages sent between trust quorum members over a sprockets channel
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerMsg {
    pub rack_id: RackUuid,
    pub kind: PeerMsgKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PeerMsgKind {
    /// Sent from a coordinator node to inform a peer about a new configuration
    Prepare {
        config: Configuration,
        share: Share,
    },

    /// Acknowledge a successful prepare from a coordinator
    PrepareAck(Epoch),

    /// Retrieve a configuration for a given epoch from a node. Nodes only
    /// respond if this is the current configuration and the requesting node is
    /// a member of the configuration.
    GetConfig(Epoch),

    /// A configuration returned in response to `GetConfig`
    Config(Configuration),

    /// Request a node's key share for the given epoch from that node
    GetShare(Epoch),

    /// Return a node's key share in response to a `GetShare` message
    Share {
        epoch: Epoch,
        share: Share,
    },

    // LRTQ shares are always at epoch 0
    GetLrtqShare,

    LrtqShare(LrtqShare),

    /// Inform a node that it is no longer part of the trust quorum as of the
    /// given epoch
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
            Self::GetConfig(_) => "get_config",
            Self::Config(_) => "config",
            Self::GetShare(_) => "get_share",
            Self::Share { .. } => "share",
            Self::GetLrtqShare => "get_lrtq_share",
            Self::LrtqShare(_) => "lrtq_share",
            Self::Expunged(_) => "expunged",
            Self::CommitAdvance(_) => "commit_advance",
        }
    }
}
