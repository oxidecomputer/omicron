// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of the oxide rack trust quorum protocol
//!
//! This protocol is written as a
//! [no-IO](https://sans-io.readthedocs.io/how-to-sans-io.html) implementation.
//! All persistent state and all networking is managed outside of this
//! implementation.

use crypto::Sha3_256Digest;
use daft::Diffable;
use derive_more::Display;
use gfss::shamir::Share;
use serde::{Deserialize, Serialize};
use slog::{Logger, error, warn};

mod compute_key_share;
mod configuration;
mod coordinator_state;
pub(crate) mod crypto;
mod messages;
mod node;
mod node_ctx;
mod persistent_state;
mod validators;
pub use configuration::Configuration;
pub use coordinator_state::{
    CoordinatorOperation, CoordinatorState, CoordinatorStateDiff,
};
pub use validators::ValidatedReconfigureMsgDiff;
mod alarm;

pub use alarm::Alarm;
pub use crypto::RackSecret;
pub use messages::*;
pub use node::{Node, NodeDiff};
// public only for docs.
pub use node_ctx::NodeHandlerCtx;
pub use node_ctx::{NodeCallerCtx, NodeCommonCtx, NodeCtx, NodeCtxDiff};
pub use persistent_state::{PersistentState, PersistentStateSummary};

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Display,
    Diffable,
)]
#[daft(leaf)]
pub struct Epoch(pub u64);

impl Epoch {
    pub fn next(&self) -> Epoch {
        Epoch(self.0.checked_add(1).expect("fewer than 2^64 epochs"))
    }
}

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
    Diffable,
)]
#[daft(leaf)]
pub struct Threshold(pub u8);

/// A unique identifier for a given trust quorum member.
//
/// This data is derived from the subject common name in the platform identity
/// certificate that makes up part of the certificate chain used to establish
/// [sprockets](https://github.com/oxidecomputer/sprockets) connections.
///
/// See RFDs 303 and 308 for more details.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Diffable,
)]
#[daft(leaf)]
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
#[derive(Debug, Clone, Serialize, Deserialize, Diffable)]
#[cfg_attr(feature = "danger_partial_eq_ct_wrapper", derive(PartialEq, Eq))]
#[daft(leaf)]
pub struct Envelope {
    pub to: PlatformId,
    pub from: PlatformId,
    pub msg: PeerMsg,
}

/// Check if a received share is valid for a given configuration
///
/// Return true if valid, false otherwise.
pub fn validate_share(
    log: &Logger,
    config: &Configuration,
    from: &PlatformId,
    epoch: Epoch,
    share: &Share,
) -> bool {
    // Are we trying to retrieve shares for `epoch`?
    if epoch != config.epoch {
        warn!(
            log,
            "Received Share from node with wrong epoch";
            "received_epoch" => %epoch,
            "from" => %from
        );
        return false;
    }

    // Is the sender a member of the configuration `epoch`?
    // Was the sender a member of the configuration at `old_epoch`?
    let Some(expected_digest) = config.members.get(&from) else {
        warn!(
            log,
            "Received Share from unexpected node";
            "epoch" => %epoch,
            "from" => %from
        );
        return false;
    };

    // Does the share hash match what we expect?
    let mut digest = Sha3_256Digest::default();
    share.digest::<sha3::Sha3_256>(&mut digest.0);
    if digest != *expected_digest {
        error!(
            log,
            "Received share with invalid digest";
            "epoch" => %epoch,
            "from" => %from
        );
        return false;
    }

    true
}
