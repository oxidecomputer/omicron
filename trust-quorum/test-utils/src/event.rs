// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Events passed to our SUT/Nexus sim in both proptests and tqdb

use crate::nexus::{NexusConfig, NexusReply};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use trust_quorum::{Epoch, PlatformId};

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
    /// Pull an envelope off the bootstrap network and call `Node::handle`
    DeliverEnvelope {
        destination: PlatformId,
    },
    /// Pull a `NexusReply` off the underlay network and update the `NexusState`
    DeliverNexusReply,
    CommitConfiguration(PlatformId),
    Reconfigure(NexusConfig),
}
