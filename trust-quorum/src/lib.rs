// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Implementation of the oxide rack trust quorum protocol

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::RangeBounds;
use std::time::Instant;
use uuid::Uuid;

// Each share is a point on a polynomial (Curve25519). Each share is 33 bytes
// - one identifier (x-coordinate) byte, and one 32-byte y-coordinate.
const SHARE_SIZE: usize = 33;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RackId(Uuid);

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Epoch(u64);

/// The number of shares required to reconstruct the rack secret
///
/// Typically referred to as `k` in the docs
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Threshold(pub u8);

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BaseboardId {
    part_number: String,
    serial_number: String,
}

/// An API request to a node to start coordinating a reconfiguration
///
/// This is a message sent by Nexus in a real deployment
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Reconfigure {
    pub epoch: Epoch,
    pub last_committed_epoch: Epoch,
    pub members: BTreeSet<BaseboardId>,
    pub threshold: Threshold,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PrepareMsg {
    config: Configuration,
    share: Share,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Envelope {
    to: BaseboardId,
    from: BaseboardId,
    msg: Msg,
}

/// A message that is sent between peers until all healthy peers have seen it
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct GossipMsg {
    epoch: Epoch,

    /// A bitmap of which nodes have so far committed the configuration
    /// for `epoch`. This order, and number, of bits matches that of the
    /// configuration for `epoch`.
    committed_bitmap: u32,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Msg {
    Prepare(PrepareMsg),
    Commit(CommitMsg),
    GossipMsg(GossipMsg),

    // TODO: Fill in
    GetShare,
    Share,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct CommitMsg {
    epoch: Epoch,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EncryptedShares(pub Vec<u8>);
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Share(pub Vec<u8>);

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ShareDigest(Sha3_256Digest);

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct Sha3_256Digest([u8; 32]);

/// The configuration for a given epoch
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Configuration {
    pub rack_uuid: RackId,
    pub epoch: Epoch,
    pub last_committed_epoch: Epoch,

    // We pick the first member of epoch 0 as coordinator when initializing from lrtq
    pub coordinator: Option<BaseboardId>,
    pub members: BTreeMap<BaseboardId, ShareDigest>,
    pub threshold: Threshold,

    // There is no encrypted data for epoch 0
    pub encrypted: Option<EncryptedData>,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EncryptedData {
    /// The encrypted key shares for the current epoch in the same order as
    /// `members`
    pub encrypted_shares: EncryptedShares,

    /// A random value used to derive the key to encrypt the shares
    ///
    /// We only encrypt the shares once and so we use a nonce of all zeros
    pub encrypted_shares_salt: [u8; 32],

    /// The encrypted rack secert for the last committed epoch
    /// `members`
    pub encrypted_last_committed_rack_secret: EncryptedShares,

    /// A random value used to derive the key to encrypt the rack secret from
    /// the last committed epoch
    ///
    /// We only encrypt the shares once and so we use a nonce of all zeros
    pub encrypted_last_committed_rack_secret_salt: [u8; 32],
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct State {
    pub node: BaseboardId,
    pub current_config: Configuration,
    pub configurations: BTreeMap<Epoch, Configuration>,
    pub prepares: BTreeMap<Epoch, PrepareMsg>,
    pub commits: BTreeMap<Epoch, CommitMsg>,
}

// TODO: thiserror
#[derive(Debug)]
pub struct ReconfigurationError {}

/// A node capable of participating in trust quorum
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Node {
    id: BaseboardId,
    state: Option<State>,
    outgoing: Vec<Envelope>,
}

impl Node {
    pub fn new(id: BaseboardId) -> Node {
        Node { id, state: None, outgoing: Vec::new() }
    }

    pub fn init_from_lrtq() -> Node {
        todo!()
    }

    pub fn id(&self) -> &BaseboardId {
        &self.id
    }

    pub fn start_reconfiguration(
        &mut self,
        msg: Reconfigure,
    ) -> Result<impl Iterator<Item = Envelope> + '_, ReconfigurationError> {
        // TODO: Everything else
        Ok(self.outgoing.drain(..))
    }

    pub fn handle_msg(
        &mut self,
        msg: Msg,
    ) -> impl Iterator<Item = Envelope> + '_ {
        // TODO: Everything else
        self.outgoing.drain(..)
    }
}
