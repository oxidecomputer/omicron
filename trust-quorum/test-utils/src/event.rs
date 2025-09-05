// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Events passed to our SUT/Nexus sim in both proptests and tqdb

use crate::nexus::{NexusConfig, NexusReply};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use trust_quorum::{Envelope, Epoch, PlatformId};

/// An event that can be fed into our system under test (SUT)
///
/// Proptest generated `Action`s get translated into events at test execution
/// time and recorded for replay.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Event {
    InitialSetup {
        member_universe_size: usize,
        config: NexusConfig,
        crashed_nodes: BTreeSet<PlatformId>,
    },
    AbortConfiguration(Epoch),
    SendNexusReplyOnUnderlay(NexusReply),
    /// Call `Node::handle` with the given Envelope.
    ///
    /// Since replay is deterministic, we actually know what this value is,
    /// even though a prior event may not have yet sent the message.
    DeliverEnvelope(Envelope),
    LoadRackSecret(PlatformId, Epoch),
    ClearSecrets(PlatformId),
    /// Pull a `NexusReply` off the underlay network and update the `NexusState`
    DeliverNexusReply(NexusReply),
    CommitConfiguration(PlatformId),
    Reconfigure(NexusConfig),
}

impl Event {
    /// Return which nodes the event may have mutated.
    pub fn affected_nodes(&self) -> Vec<PlatformId> {
        match self {
            Self::InitialSetup { config, crashed_nodes, .. } => {
                config.members.union(&crashed_nodes).cloned().collect()
            }
            Self::AbortConfiguration(_) => vec![],
            Self::SendNexusReplyOnUnderlay(_) => vec![],
            Self::DeliverEnvelope(envelope) => vec![envelope.to.clone()],
            Self::DeliverNexusReply(_) => vec![],
            Self::LoadRackSecret(id, _) => vec![id.clone()],
            Self::ClearSecrets(id) => vec![id.clone()],
            Self::CommitConfiguration(id) => vec![id.clone()],
            Self::Reconfigure(_) => vec![],
        }
    }
}
