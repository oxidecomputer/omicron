// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of the oxide rack trust quorum protocol
//!
//! This protocol is written as a
//! [no-IO](https://sans-io.readthedocs.io/how-to-sans-io.html) implementation.
//! All persistent state and all networking is managed outside of this
//! implementation.

use derive_more::Display;
use serde::{Deserialize, Serialize};

mod configuration;
mod coordinator_state;
pub(crate) mod crypto;
mod messages;
mod node;
mod node_ctx;
mod persistent_state;
mod validators;
pub use configuration::Configuration;
pub(crate) use coordinator_state::CoordinatorState;
pub use crypto::RackSecret;
pub use messages::*;
pub use node::Node;
// public only for docs.
pub use node_ctx::NodeHandlerCtx;
pub use node_ctx::{NodeCallerCtx, NodeCommonCtx, NodeCtx};
pub use persistent_state::{PersistentState, PersistentStateSummary};

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Display,
)]
pub struct Epoch(pub u64);

/// The number of shares required to reconstruct the rack secret
///
/// Typically referred to as `k` in the docs
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Display,
)]
pub struct Threshold(pub u8);

/// A unique identifier for a given trust quorum member.
//
/// This data is derived from the subject common name in the platform identity
/// certificate that makes up part of the certificate chain used to establish
/// [sprockets](https://github.com/oxidecomputer/sprockets) connections.
///
/// See RFDs 303 and 308 for more details.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct PlatformId {
    part_number: String,
    serial_number: String,
}

impl std::fmt::Display for PlatformId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.part_number, self.serial_number)
    }
}

impl PlatformId {
    pub fn new(part_number: String, serial_number: String) -> PlatformId {
        PlatformId { part_number, serial_number }
    }

    pub fn part_number(&self) -> &str {
        &self.part_number
    }

    pub fn serial_number(&self) -> &str {
        &self.serial_number
    }
}

/// A container to make messages between trust quorum nodes routable
#[derive(Debug, Serialize, Deserialize)]
pub struct Envelope {
    pub to: PlatformId,
    pub from: PlatformId,
    pub msg: PeerMsg,
}
