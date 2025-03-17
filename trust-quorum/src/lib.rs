// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of the oxide rack trust quorum protocol
//!
//! This protocol is written as a
//! [no-IO](https://sans-io.readthedocs.io/how-to-sans-io.html) implementation.
//! All persistent state and all networking is managed outside of this
//! implementation. Callers interact with the protocol via the [`Node`] api
//! which itself performs "environment" operations such as persisting state and
//! sending messages via [`Output`] messages.

use rack_secret::RackSecret;
use serde::{Deserialize, Serialize};
use sha3::{Digest, Sha3_256};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;
use uuid::Uuid;
use zeroize::ZeroizeOnDrop;

mod configuration;
mod messages;
mod node;
mod persistent_state;
mod rack_secret;
pub use configuration::Configuration;
pub use messages::*;
pub use node::Node;
pub use persistent_state::{
    DecommissionedMetadata, LrtqLedgerData, PersistentState,
};

// Each share is a point on a polynomial (Curve25519). Each share is 33 bytes
// - one identifier (x-coordinate) byte, and one 32-byte y-coordinate.
const SHARE_SIZE: usize = 33;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct RackId(Uuid);

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct Epoch(u64);

/// The number of shares required to reconstruct the rack secret
///
/// Typically referred to as `k` in the docs
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
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

/// A container to make messages between trust quorum nodes routable
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Envelope {
    to: PlatformId,
    from: PlatformId,
    msg: PeerMsg,
}

// The output of a given API call
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Output {
    Envelope(Envelope),
    PersistPrepare(PrepareMsg),
    PersistCommit(CommitMsg),
    PersistDecommissioned { from: PlatformId, epoch: Epoch },
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct EncryptedRackSecret(pub Vec<u8>);

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ZeroizeOnDrop,
)]
pub struct Share(Vec<u8>);

// We don't want to risk debug-logging the actual share contents, so implement
// `Debug` manually.
impl std::fmt::Debug for Share {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Share").finish()
    }
}

impl Share {
    pub fn new(share: Vec<u8>) -> Share {
        assert_eq!(share.len(), SHARE_SIZE);
        Share(share)
    }
}

impl From<&Share> for ShareDigest {
    fn from(share: &Share) -> Self {
        ShareDigest(Sha3_256Digest(
            Sha3_256::digest(&share.0).as_slice().try_into().unwrap(),
        ))
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct ShareDigest(Sha3_256Digest);

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct Sha3_256Digest([u8; 32]);

/// The state of a reconfiguration coordinator.
///
/// A coordinator can be any trust quorum node that is a member of both the old
/// and new group. The coordinator is chosen by Nexus for a given epoch when a
/// trust quorum reconfiguration is triggered. Reconfiguration is only performed
/// when the control plane is up, as we use Nexus to persist prepares and ensure
/// commitment happens, even if the system crashes while committing.  If a
/// rack crash (such as a power outage) occurs before nexus is informed of the
/// prepares, nexus will  skip the epoch and start a new reconfiguration. This
/// allows progress to always be made with a full linearization of epochs.
pub struct CoordinatorState {
    start_time: Instant,
    // We copy the last committed reconfiguration here so that decisions
    // can be made with only the state local to this `CoordinatorState`.
    // If we get a prepare message or commit message with a later epoch
    // we will abandon this coordinator state.
    last_committed_configuration: Option<Configuration>,
    reconfigure: Reconfigure,
    // Received key shares for the last committed epoch
    // Only used if there actually is a last_committed_epoch
    received_key_shares: BTreeMap<PlatformId, Share>,
    // Once we've received enough key shares for the last committed epoch (if
    // there is one), we can reconstruct the rack secret and drop the shares.
    last_committed_rack_secret: Option<RackSecret>,
    // Once we've recreated the rack secret for the last committed epoch
    // we will generate a rack secret and split it into key shares.
    // then populate a `PrepareMsg` for each member. When the member acknowledges
    // receipt, we will remove the prepare message. Once all members acknowledge
    // their prepare, we are done and can respond to nexus.
    prepares: BTreeMap<PlatformId, PrepareMsg>,
    prepare_acks: BTreeSet<PlatformId>,

    // The next retry timeout deadline
    retry_deadline: Instant,
}

impl CoordinatorState {
    pub fn new(
        now: Instant,
        reconfigure: Reconfigure,
        last_committed_configuration: Option<Configuration>,
    ) -> CoordinatorState {
        let retry_deadline = now + reconfigure.retry_timeout;
        CoordinatorState {
            start_time: now,
            last_committed_configuration,
            reconfigure,
            received_key_shares: BTreeMap::default(),
            last_committed_rack_secret: None,
            prepares: BTreeMap::default(),
            prepare_acks: BTreeSet::default(),
            retry_deadline,
        }
    }

    /// Return true if the coordinator is sending Prepares and awaiting acks
    pub fn is_preparing(&self) -> bool {
        self.last_committed_configuration.is_none()
            || self.last_committed_rack_secret.is_some()
    }

    /// Do we have a key share for the given node?
    pub fn has_key_share(&self, node: &PlatformId) -> bool {
        self.received_key_shares.contains_key(node)
    }
}
