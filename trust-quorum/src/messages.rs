// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Messsages for the trust quorum protocol

use crate::crypto::LrtqShare;
use crate::{Configuration, Epoch, PlatformId, Threshold};
use gfss::shamir::Share;
use omicron_uuid_kinds::RackUuid;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, time::Duration};

/// A request from nexus informing a node to start coordinating a
/// reconfiguration.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ReconfigureMsg {
    pub rack_id: RackUuid,
    pub epoch: Epoch,
    pub last_committed_epoch: Option<Epoch>,
    pub members: BTreeSet<PlatformId>,
    pub threshold: Threshold,

    // The timeout before we send a follow up request to a peer
    pub retry_timeout: Duration,
}

/// Messages sent between trust quorum members over a sprockets channel
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PeerMsg {
    Prepare(PrepareMsg),
    PrepareAck(Epoch),
    Commit(CommitMsg),

    GetShare(Epoch),
    Share { epoch: Epoch, share: Share },

    // LRTQ shares are always at epoch 0
    GetLrtqShare,
    LrtqShare(LrtqShare),
}

/// The start of a reconfiguration sent from a coordinator to a specific peer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrepareMsg {
    pub config: Configuration,
    pub share: Share,
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct CommitMsg {
    epoch: Epoch,
}
