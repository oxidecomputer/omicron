// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Messsages for the trust quorum protocol

use crate::crypto::{KeyShareEd25519, KeyShareGf256};
use crate::{Configuration, Epoch, PlatformId, RackId, Threshold};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, time::Duration};

/// A request from nexus informing a node to start coordinating a
/// reconfiguration
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ReconfigureMsg {
    pub rack_id: RackId,
    pub epoch: Epoch,
    pub last_committed_epoch: Option<Epoch>,
    pub members: BTreeSet<PlatformId>,
    pub threshold: Threshold,

    // The timeout before we send a follow up request to a peer
    pub retry_timeout: Duration,
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct PrepareMsg {
    pub config: Configuration,
    pub share: KeyShareGf256,
}

/// A message that is sent between peers until all healthy peers have seen it
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct CommittedMsg {
    pub epoch: Epoch,
}

/// Messages sent between trust quorum members over a sprockets channel
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub enum PeerMsg {
    Prepare(PrepareMsg),
    PrepareAck(Epoch),
    Commit(CommitMsg),
    Committed(CommittedMsg),

    GetShare(Epoch),
    Share { epoch: Epoch, share: KeyShareGf256 },

    // LRTQ shares are always at epoch 0
    GetLrtqShare,
    LrtqShare(KeyShareEd25519),
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct CommitMsg {
    epoch: Epoch,
}
