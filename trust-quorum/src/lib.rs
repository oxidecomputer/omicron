// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Implementation of the oxide rack trust quorum protocol

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;
use uuid::Uuid;

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

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
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

/// Requests received from Nexus
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum NexusReq {
    /// Retrieve the hash of a share for an LRTQ node
    /// This is necessary when coordinating upgrades
    GetLrtqShareHash,

    /// Inform a member to upgrade from LRTQ by creating a new PrepareMsg for
    /// epoch 0 and persisting it.
    UpgradeFromLrtq(UpgradeFromLrtqMsg),

    /// If the upgrade has not yet been activated, then it can be cancelled
    /// and tried again.
    CancelUpgradeFromLrtq(CancelUpgradeFromLrtqMsg),
}

/// Responses to Nexus Requests
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum NexusRsp {}

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

// The output of a given API call
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Output {
    Envelope(Envelope),
    NexusRsp(NexusRsp),
    PersistPrepare(PrepareMsg),
    PersistCommit(CommitMsg),
    PersistDecommissioned { from: BaseboardId, epoch: Epoch },
}

/// A message that is sent between peers until all healthy peers have seen it
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct GossipMsg {
    epoch: Epoch,

    /// A bitmap of which nodes have so far committed the configuration
    /// for `epoch`. This order, and number, of bits matches that of the
    /// configuration for `epoch`. Members fill in their own
    /// bit after they have committed.
    committed_bitmap: u32,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct UpgradeFromLrtqMsg {
    pub upgrade_id: Uuid,
    pub members: BTreeSet<BaseboardId>,
    pub share_digests: BTreeSet<Sha3_256Digest>,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct CancelUpgradeFromLrtqMsg {
    pub upgrade_id: Uuid,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Msg {
    Prepare(PrepareMsg),
    Commit(CommitMsg),
    GossipMsg(GossipMsg),

    // TODO: Fill in
    GetShare,
    Share,

    // A member that was offline while a reconfiguration was taking place can
    // request its `PrepareMsg` from another member. A node will know to request
    // its Prepare message if it sees a Commit for a `Prepare` that it doesn't have
    // or it sees a `GossipMsg` for a commit and thinks it might be a peer
    GetPrepare,

    /// A response from a `GetPrepare` request indicating that the peer is not a
    /// member of the group for a given epoch.
    NotAMember(Epoch),
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

    /// We pick the first member of epoch 0 as coordinator when initializing from
    /// lrtq so we don't have to use an option
    pub coordinator: BaseboardId,
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
    pub configurations: BTreeMap<Epoch, Configuration>,
    pub prepares: BTreeMap<Epoch, PrepareMsg>,
    pub commits: BTreeMap<Epoch, CommitMsg>,

    // Has the node received a `NotAMember` message for the latest epoch? If at
    // any time this gets set, than the it remains true for the lifetime of the
    // node. The sled corresponding to the node must be factory reset by wiping
    // its storage.
    pub decommissioned: bool,
}

// TODO: thiserror
#[derive(Debug)]
pub enum ReconfigurationError {
    StaleEpoch(Epoch),
}

/// A node capable of participating in trust quorum
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Node {
    id: BaseboardId,
    state: Option<State>,
    outgoing: Vec<Output>,
}

impl Node {
    pub fn new(id: BaseboardId) -> Node {
        Node { id, state: None, outgoing: Vec::new() }
    }

    pub fn id(&self) -> &BaseboardId {
        &self.id
    }

    pub fn start_reconfiguration(
        &mut self,
        msg: Reconfigure,
    ) -> Result<impl Iterator<Item = Output> + '_, ReconfigurationError> {
        if let Some(state) = &self.state {
            // If we have a `State`, we must have at least one `Prepare`
            let highest_epoch = state.prepares.keys().last().unwrap();
            if msg.epoch <= *highest_epoch {
                return Err(ReconfigurationError::StaleEpoch(msg.epoch));
            }
        }

        // TODO: Everything else
        Ok(self.outgoing.drain(..))
    }

    pub fn handle_msg(
        &mut self,
        msg: Msg,
    ) -> impl Iterator<Item = Output> + '_ {
        // TODO: Everything else
        self.outgoing.drain(..)
    }

    pub fn tick(&mut self, now: Instant) -> impl Iterator<Item = Output> + '_ {
        // TODO: Everything else
        self.outgoing.drain(..)
    }

    fn send(&mut self, to: BaseboardId, msg: Msg) {
        self.outgoing.push(Output::Envelope(Envelope {
            to,
            from: self.id.clone(),
            msg,
        }));
    }

    fn persist_prepare(&mut self, msg: PrepareMsg) {
        self.outgoing.push(Output::PersistPrepare(msg));
    }

    fn persist_commit(&mut self, msg: CommitMsg) {
        self.outgoing.push(Output::PersistCommit(msg));
    }

    fn persist_decomissioned(&mut self, from: BaseboardId, epoch: Epoch) {
        self.outgoing.push(Output::PersistDecommissioned { from, epoch });
    }
}
