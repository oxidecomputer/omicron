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
use std::collections::BTreeMap;
use std::time::Instant;
use uuid::Uuid;
use zeroize::ZeroizeOnDrop;

mod configuration;
mod messages;
mod rack_secret;
pub use configuration::Configuration;
pub use messages::*;

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

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecommissionedMetadata {
    /// The committed epoch, later than its current configuration at which the
    /// node learned that it had been decommissioned.
    epoch: Epoch,

    /// Which node this commit information was learned from  
    from: PlatformId,
}

/// Data loaded from the ledger by sled-agent on instruction from Nexus
///
/// The epoch is always 0, because LRTQ does not allow key-rotation
///
/// Technically, this is persistant state, but it is not altered by this
/// protocol and therefore it is not part of `PersistentState`.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct LrtqLedgerData {
    pub rack_uuid: RackId,
    pub threshold: Threshold,
    pub share: Share,
}

/// All the persistent state for this protocol
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PersistentState {
    pub prepares: BTreeMap<Epoch, PrepareMsg>,
    pub commits: BTreeMap<Epoch, CommitMsg>,

    // Has the node seen a commit for an epoch higher than it's current
    // configuration for which it has not received a `PrepareMsg` for? If at
    // any time this gets set, than the it remains true for the lifetime of the
    // node. The sled corresponding to the node must be factory reset by wiping
    // its storage.
    pub decommissioned: Option<DecommissionedMetadata>,
}

impl PersistentState {
    pub fn last_prepared_epoch(&self) -> Option<Epoch> {
        self.prepares.keys().last().map(|epoch| *epoch)
    }

    pub fn last_committed_epoch(&self) -> Option<Epoch> {
        self.commits.keys().last().map(|epoch| *epoch)
    }

    // Get the configuration for the current epoch from its prepare message
    pub fn configuration(&self, epoch: Epoch) -> Option<&Configuration> {
        self.prepares.get(&epoch).map(|p| &p.config)
    }

    pub fn last_committed_reconfiguration(&self) -> Option<&Configuration> {
        self.last_committed_epoch().map(|epoch| {
            // There *must* be a prepare if we have a commit
            self.configuration(epoch).expect("missing prepare")
        })
    }
}

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

/// A node capable of participating in trust quorum
pub struct Node {
    id: PlatformId,
    persistent_state: PersistentState,
    outgoing: Vec<Output>,

    // If this node was an LRTQ node, sled-agent will start it with the ledger
    // data it read from disk. This allows us to upgrade from LRTQ.
    lrtq_ledger_data: Option<LrtqLedgerData>,

    // The state of a coordinator performing a reconfiguration
    coordinator_state: Option<CoordinatorState>,
}

impl Node {
    pub fn new(
        id: PlatformId,
        lrtq_ledger_data: Option<LrtqLedgerData>,
    ) -> Node {
        Node {
            id,
            persistent_state: PersistentState::default(),
            outgoing: Vec::new(),
            lrtq_ledger_data,
            coordinator_state: None,
        }
    }

    pub fn id(&self) -> &PlatformId {
        &self.id
    }

    /// Become a coordinator of a reconfiguration for a given epoch.
    ///
    /// Generate a new rack secret for the given epoch, encrypt the old one with
    /// a key derived from the new rack secret, split the rack secret, create a
    /// bunch of `PrepareMsgs` and send them to peer nodes.
    pub fn coordinate_reconfiguration(
        &mut self,
        now: Instant,
        msg: Reconfigure,
    ) -> Result<(), Error> {
        self.check_membership_constraints(msg.members.len(), msg.threshold)?;
        self.check_in_service()?;

        let last_committed_epoch = self.persistent_state.last_committed_epoch();

        // We can only reconfigure if the current epoch matches the last
        // committed epoch in the `Reconfigure` request.
        if msg.last_committed_epoch != last_committed_epoch {
            return Err(Error::LastCommittedEpochMismatch {
                node_epoch: last_committed_epoch,
                msg_epoch: msg.last_committed_epoch,
            });
        }

        // We must not have seen a prepare for this epoch or any greater epoch
        if let Some(last_prepared_epoch) =
            self.persistent_state.last_prepared_epoch()
        {
            if msg.epoch <= last_prepared_epoch {
                return Err(Error::PreparedEpochMismatch {
                    existing: last_prepared_epoch,
                    new: msg.epoch,
                });
            }
        }

        // If we are already coordinating, we must abandon that coordination.
        // TODO: Logging?
        let mut coordinator_state = CoordinatorState::new(
            now,
            msg,
            self.persistent_state.last_committed_reconfiguration().cloned(),
        );

        // Start collecting shares for `last_committed_epoch`
        self.collect_shares_as_coordinator(&mut coordinator_state);

        // Save the coordinator state in `self`
        self.coordinator_state = Some(coordinator_state);

        Ok(())
    }

    pub fn handle_peer_msg(
        &mut self,
        from: PlatformId,
        msg: PeerMsg,
    ) -> impl Iterator<Item = Output> + '_ {
        match msg {
            PeerMsg::Prepare(_) => todo!(),
            PeerMsg::PrepareAck(epoch) => self.handle_prepare_ack(from, epoch),
            PeerMsg::Commit(_) => todo!(),
            PeerMsg::Committed(_) => todo!(),
            PeerMsg::GetShare(_) => todo!(),
            PeerMsg::Share { .. } => todo!(),
        }
        self.outgoing.drain(..)
    }

    pub fn tick(&mut self, now: Instant) -> impl Iterator<Item = Output> + '_ {
        self.tick_coordinator(now);

        // TODO: Everything else
        self.outgoing.drain(..)
    }

    // TODO: Logging?
    // We only handle this message if we are coordinating for `epoch`
    fn handle_prepare_ack(&mut self, from: PlatformId, epoch: Epoch) {
        let Some(mut coordinator_state) = self.coordinator_state.take() else {
            // Stale ack.
            return;
        };
        if coordinator_state.reconfigure.epoch == epoch {
            coordinator_state.prepares.remove(&from);
            // TODO: Keep track of acks, rather than just removing prepares
            // Nexus polls us, so we need to tell it who acked explicitly
            // It will wait for K+Z acks as described in RFD 238 sec 5.
        }

        self.coordinator_state = Some(coordinator_state);
    }

    /// Send a `GetShare` request for every member in the last committed epoch
    /// that has not yet returned a share. This is so that we can recreate the
    /// rack secret of the last committed epoch and encrypt it with the new rack
    /// secret to enable key rotation.
    fn collect_shares_as_coordinator(
        &mut self,
        coordinator_state: &mut CoordinatorState,
    ) {
        if let Some(last_committed_config) =
            &coordinator_state.last_committed_configuration
        {
            for (member, _) in &last_committed_config.members {
                // Send a `GetShare` request if we haven't received a key
                // share from this node yet.
                if !coordinator_state.has_key_share(member) {
                    self.send_to_peer(
                        member.clone(),
                        PeerMsg::GetShare(last_committed_config.epoch),
                    );
                }
            }
        }
    }

    /// Send any required messages due to a timeout
    fn tick_coordinator(&mut self, now: Instant) {
        let Some(mut coordinator_state) = self.coordinator_state.take() else {
            // Not currently a coordinator
            return;
        };
        self.tick_coordinator_impl(now, &mut coordinator_state);

        // Put the coordinator state back into `self`.
        self.coordinator_state = Some(coordinator_state);
    }

    // A helper method that operates directly on the coordinator state without
    // worrying about putting it back into `self`. This is specifically to work
    // around the borrow checker.
    fn tick_coordinator_impl(
        &mut self,
        now: Instant,
        coordinator_state: &mut CoordinatorState,
    ) {
        if now < coordinator_state.retry_deadline {
            // Nothing to do.
            return;
        }

        // Resend any necessary messages to peer nodes

        // Are we currently trying to receive key shares or prepare acks?
        if !coordinator_state.is_preparing() {
            self.collect_shares_as_coordinator(coordinator_state);
        } else {
            for (node, prepare) in &coordinator_state.prepares {
                self.send_to_peer(
                    node.clone(),
                    PeerMsg::Prepare(prepare.clone()),
                );
            }
        }

        // Reset retry_deadline
        coordinator_state.retry_deadline =
            now + coordinator_state.reconfigure.retry_timeout;
    }

    /// Verify that the node is not decommissioned
    fn check_in_service(&mut self) -> Result<(), Error> {
        if let Some(decommissioned) = &self.persistent_state.decommissioned {
            return Err(Error::SledDecommissioned {
                from: decommissioned.from.clone(),
                epoch: decommissioned.epoch,
                last_prepared_epoch: self
                    .persistent_state
                    .last_prepared_epoch(),
            });
        }
        Ok(())
    }

    /// Verify that the cluster membership and threshold sizes are within
    /// constraints
    fn check_membership_constraints(
        &mut self,
        num_members: usize,
        threshold: Threshold,
    ) -> Result<(), Error> {
        if num_members <= threshold.0 as usize {
            return Err(Error::MembershipThresholdMismatch {
                num_members,
                threshold,
            });
        }

        if num_members < 3 || num_members > 32 {
            return Err(Error::InvalidMembershipSize(num_members));
        }

        if threshold.0 < 2 || threshold.0 > 31 {
            return Err(Error::InvalidThreshold(threshold));
        }

        Ok(())
    }

    fn send_to_peer(&mut self, to: PlatformId, msg: PeerMsg) {
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

    fn persist_decomissioned(&mut self, from: PlatformId, epoch: Epoch) {
        self.outgoing.push(Output::PersistDecommissioned { from, epoch });
    }
}
