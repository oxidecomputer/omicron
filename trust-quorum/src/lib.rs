// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of the oxide rack trust quorum protocol

use rack_secret::RackSecret;
use serde::{Deserialize, Serialize};
use sha3::{Digest, Sha3_256};
use std::collections::{BTreeMap, BTreeSet};
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

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct BaseboardId {
    part_number: String,
    serial_number: String,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Envelope {
    to: BaseboardId,
    from: BaseboardId,
    msg: PeerMsg,
}

// The output of a given API call
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Output {
    Envelope(Envelope),
    NexusRsp(NexusRsp),
    PersistPrepare(PrepareMsg),
    PersistCommit(CommitMsg),
    PersistDecommissioned { from: BaseboardId, epoch: Epoch },
    PersistLrtqCancelled { lrtq_upgrade_id: Uuid },
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
    from: BaseboardId,
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

/// The state of a reconfiguration coordinator
pub struct CoordinatorState {
    nexus_request_id: Uuid,
    start_time: Instant,
    // We copy the last committed reconfiguration here so that decisions
    // can be made with only the state local to this `CoordinatorState`.
    // If we get a prepare message or commit message with a later epoch
    // we will abandon this coordinator state.
    last_committed_configuration: Option<Configuration>,
    reconfigure: Reconfigure,
    // Received key shares for the last committed epoch
    // Only used if there actually is a last_committed_epoch
    received_key_shares: BTreeMap<BaseboardId, Share>,
    // Once we've received enough key shares for the last committed epoch (if
    // there is one), we can reconstruct the rack secret and drop the shares.
    last_committed_rack_secret: Option<RackSecret>,
    // Once we've recreated the rack secret for the last committed epoch
    // we will generate a rack secret and split it into key shares.
    // then populate a `PrepareMsg` for each member. When the member acknowledges
    // receipt, we will remove the prepare message. Once all members acknowledge
    // their prepare, we are done and can respond to nexus.
    prepares: BTreeMap<BaseboardId, PrepareMsg>,

    // The next retry timeout deadline
    retry_deadline: Instant,
}

impl CoordinatorState {
    pub fn new(
        nexus_request_id: Uuid,
        now: Instant,
        reconfigure: Reconfigure,
        last_committed_configuration: Option<Configuration>,
    ) -> CoordinatorState {
        let retry_deadline = now + reconfigure.retry_timeout;
        CoordinatorState {
            nexus_request_id,
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

    /// Has the entire coordination timed out?
    pub fn is_expired(&self, now: Instant) -> bool {
        now > self.start_time + self.reconfigure.timeout
    }

    /// Do we have a key share for the given node?
    pub fn has_key_share(&self, node: &BaseboardId) -> bool {
        self.received_key_shares.contains_key(node)
    }
}

/// A node capable of participating in trust quorum
pub struct Node {
    id: BaseboardId,
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
        id: BaseboardId,
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

    pub fn id(&self) -> &BaseboardId {
        &self.id
    }

    pub fn handle_nexus_request(
        &mut self,
        now: Instant,
        NexusReq { id, kind }: NexusReq,
    ) -> impl Iterator<Item = Output> + '_ {
        // All errors are solely for early return purposes.
        // The actual errors are sent to nexus as replies.
        let _ = match kind {
            NexusReqKind::Reconfigure(msg) => {
                self.coordinate_reconfiguration(id, now, msg)
            }
            NexusReqKind::Commit(msg) => todo!(),
            NexusReqKind::GetCommitted(epoch) => todo!(),
            NexusReqKind::GetLrtqShareHash => self.get_lrtq_share_digest(id),
            NexusReqKind::UpgradeFromLrtq(msg) => {
                self.upgrade_from_lrtq(id, msg)
            }
            NexusReqKind::CancelUpgradeFromLrtq(msg) => {
                self.cancel_upgrade_from_lrtq(id, msg)
            }
        };
        self.outgoing.drain(..)
    }

    pub fn handle_peer_msg(
        &mut self,
        from: BaseboardId,
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
    fn handle_prepare_ack(&self, from: BaseboardId, epoch: Epoch) {
        // We only handle this message if we are coordinating for `epoch`
        if let Some(coordinator_state) = &mut self.coordinator_state {
            if coordinator_state.reconfigure.epoch == epoch {
                coordinator_state.prepares.remove(&from);
                if coordinator_state.prepares.is_empty() {
                    // We are done. Inform nexus of the success.
                    self.reply_to_nexus(
                        coordinator_state.nexus_request_id,
                        NexusRspKind::Prepared(epoch),
                    );
                }
            }
        }
    }

    /// Generate a new rack secret for the given epoch, encrypt the old one with
    /// a key derived from the new rack secret, split the rack secret,
    /// create a bunch of `PrepareMsgs` and send them to peer nodes.
    fn coordinate_reconfiguration(
        &mut self,
        request_id: Uuid,
        now: Instant,
        msg: Reconfigure,
    ) -> Result<(), ()> {
        self.check_in_service(request_id)?;

        let last_committed_epoch = self.persistent_state.last_committed_epoch();

        // We can only reconfigure if the current epoch matches the last
        // committed epoch in the `Reconfigure` request.
        if msg.last_committed_epoch != last_committed_epoch {
            self.reply_to_nexus(
                request_id,
                NexusRspKind::Error(
                    NexusRspError::LastCommittedEpochMismatch {
                        node_epoch: last_committed_epoch,
                        msg_epoch: msg.last_committed_epoch,
                    },
                ),
            );
            return Err(());
        }

        // We must not have seen a prepare for this epoch or any greater epoch
        if let Some(last_prepared_epoch) =
            self.persistent_state.last_prepared_epoch()
        {
            if msg.epoch <= last_prepared_epoch {
                self.reply_to_nexus(
                    request_id,
                    NexusRspKind::Error(NexusRspError::PreparedEpochMismatch {
                        existing: last_prepared_epoch,
                        new: msg.epoch,
                    }),
                );
            }
            return Err(());
        }

        // If we are already coordinating, we must abandon that coordination.
        // TODO: Logging?
        let mut coordinator_state = CoordinatorState::new(
            request_id,
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

        // Put the coordinator state back into `self`, unless it has expired
        // We perform this inexpensive check twice for safety purposes, so that
        // we never forget to put the coordinator state back if we need to. We
        // don't rely on the return value from the tick to indicate whether it
        // should be put back or not.
        if !coordinator_state.is_expired(now) {
            self.coordinator_state = Some(coordinator_state);
        }
    }

    // A helper method that operates directly on the coordinator state without
    // worrying about putting it back into `self`. This is specifically to work
    // around the borrow checker.
    fn tick_coordinator_impl(
        &mut self,
        now: Instant,
        coordinator_state: &mut CoordinatorState,
    ) {
        // Did the coordination timeout? If so, inform nexus.
        if coordinator_state.is_expired(now) {
            // TODO: logging?
            self.reply_to_nexus(
                coordinator_state.nexus_request_id,
                NexusRspKind::Timeout,
            );
            return;
        }

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

    /// If an LRTQ upgrade has not yet taken place, and this is an lrtq node
    /// then calculate the digest of this nodes lrtq share and send it to nexus.
    /// Otherwise send an error to nexus.
    fn get_lrtq_share_digest(&mut self, request_id: Uuid) -> Result<(), ()> {
        self.check_in_service(request_id)?;
        // We only allow retrieval of the lrtq share digest before an lrtq
        // upgrade has been attempted. Nexus can try again, but first it must
        // attempt to cancel the current outstanding reconfiguration. There is
        // no reason to leak any information, even a hash, if its unnecessary
        // for the application to have that information.
        self.check_no_prepares(request_id)?;
        let lrtq_ledger_data = self.get_lrtq_ledger_data(request_id)?;

        self.reply_to_nexus(
            request_id,
            NexusRspKind::LrtqShareDigest(ShareDigest::from(
                &lrtq_ledger_data.share,
            )),
        );

        Ok(())
    }

    /// Perform an upgrade from lrtq into v1 of the trust-quorum protocol
    fn upgrade_from_lrtq(
        &mut self,
        request_id: Uuid,
        msg: UpgradeFromLrtqMsg,
    ) -> Result<(), ()> {
        self.check_in_service(request_id)?;
        self.check_no_prepares(request_id)?;

        // Return early if we are not an lrtq node
        let lrtq_ledger_data = self.get_lrtq_ledger_data(request_id)?;

        self.check_membership_constraints(
            request_id,
            msg.members.len(),
            lrtq_ledger_data.threshold,
        )?;

        // Create and persist a prepare, and ack to Nexus
        let config = Configuration {
            rack_uuid: lrtq_ledger_data.rack_uuid,
            epoch: Epoch(0),
            last_committed_epoch: None,
            coordinator: msg.members.keys().next().unwrap().clone(),
            members: msg.members,
            threshold: lrtq_ledger_data.threshold,
            encrypted: None,
            lrtq_upgrade_id: Some(msg.upgrade_id),
        };

        let prepare = PrepareMsg { config, share: lrtq_ledger_data.share };
        self.persistent_state.prepares.insert(Epoch(0), prepare.clone());
        self.persist_prepare(prepare);
        self.reply_to_nexus(
            request_id,
            NexusRspKind::UpgradeFromLrtqAck { upgrade_id: msg.upgrade_id },
        );

        Ok(())
    }

    fn cancel_upgrade_from_lrtq(
        &mut self,
        request_id: Uuid,
        msg: CancelUpgradeFromLrtqMsg,
    ) -> Result<(), ()> {
        self.check_in_service(request_id)?;
        self.check_only_epoch_zero_prepared(request_id)?;

        if let Some(config) = self.persistent_state.configuration(Epoch(0)) {
            if config.lrtq_upgrade_id == Some(msg.upgrade_id) {
                // Success!
                self.persistent_state.prepares.remove(&Epoch(0));
                self.persist_lrtq_cancelled(msg.upgrade_id);
            } else {
                // Stale reconfiguration Id.
                // TODO: What should we do here?
                // Log this?
                // Replying to nexus doesn't seem to make much sense as the
                // request could be stale
            }
        }

        // No prepares or anything. Just consider this idempotent.
        Ok(())
    }

    /// Get the lrtq ledger data if this node is an lrtq node
    ///
    /// Send an error reply to nexus if not an lrtq node, and return an error.
    ///
    /// Note: The error is simply to allow early return, and conveys no
    /// information, as that is already in the reply to nexus.
    fn get_lrtq_ledger_data(
        &mut self,
        request_id: Uuid,
    ) -> Result<LrtqLedgerData, ()> {
        if let Some(lrtq_ledger_data) = &self.lrtq_ledger_data {
            Ok(lrtq_ledger_data.clone())
        } else {
            self.reply_to_nexus(
                request_id,
                NexusRspKind::Error(NexusRspError::NotAnLrtqMember),
            );
            return Err(());
        }
    }

    /// Verify that the node is not decommissioned
    ///
    /// Send a reply to nexus if it is, and return an error.
    ///
    /// Note: The error is simply to allow early return, and conveys no
    /// information, as that is already in the reply to nexus.
    fn check_in_service(&mut self, request_id: Uuid) -> Result<(), ()> {
        if let Some(decommissioned) = &self.persistent_state.decommissioned {
            self.reply_to_nexus(
                request_id,
                NexusRspKind::Error(NexusRspError::SledDecommissioned {
                    from: decommissioned.from.clone(),
                    epoch: decommissioned.epoch,
                    last_prepared_epoch: self
                        .persistent_state
                        .last_prepared_epoch(),
                }),
            );
            return Err(());
        }
        Ok(())
    }

    /// Verify that no configurations have been prepared yet
    ///
    /// Send a reply to nexus if there are any prepares, and return an error.
    ///
    /// Note: The error is simply to allow early return, and conveys no
    /// information, as that is already in the reply to nexus.
    fn check_no_prepares(&mut self, request_id: Uuid) -> Result<(), ()> {
        if let Some(epoch) = self.persistent_state.last_prepared_epoch() {
            self.reply_to_nexus(
                request_id,
                NexusRspKind::Error(NexusRspError::AlreadyPrepared(epoch)),
            );
            return Err(());
        }
        Ok(())
    }

    /// Verify that so far this node has only prepared epoch 0 and not yet committed it.
    ///
    /// Send a reply to nexus if this is false, and return an error.
    ///
    /// Note: The error is simply to allow early return, and conveys no
    /// information, as that is already in the reply to nexus.
    fn check_only_epoch_zero_prepared(
        &mut self,
        request_id: Uuid,
    ) -> Result<(), ()> {
        if let Some(epoch) = self.persistent_state.last_committed_epoch() {
            self.reply_to_nexus(
                request_id,
                NexusRspKind::Error(NexusRspError::AlreadyCommitted(epoch)),
            );
            return Err(());
        }

        if let Some(epoch) = self.persistent_state.last_prepared_epoch() {
            if epoch > Epoch(0) {
                self.reply_to_nexus(
                    request_id,
                    NexusRspKind::Error(NexusRspError::AlreadyPrepared(epoch)),
                );
            }
            return Err(());
        }

        Ok(())
    }

    /// Verify that the cluster membership and threshold sizes are within
    /// constraints
    ///
    /// Send a reply to nexus if the constraints are invalid and return an
    /// error.
    ///
    /// Note: The error is simply to allow early return, and conveys no
    /// information, as that is already in the reply to nexus.
    fn check_membership_constraints(
        &mut self,
        request_id: Uuid,
        num_members: usize,
        threshold: Threshold,
    ) -> Result<(), ()> {
        if num_members <= threshold.0 as usize {
            self.reply_to_nexus(
                request_id,
                NexusRspKind::Error(
                    NexusRspError::MembershipThresholdMismatch {
                        num_members,
                        threshold,
                    },
                ),
            );
            return Err(());
        }

        if num_members < 3 || num_members > 32 {
            self.reply_to_nexus(
                request_id,
                NexusRspKind::Error(NexusRspError::InvalidMembershipSize(
                    num_members,
                )),
            );
            return Err(());
        }

        if threshold.0 < 2 || threshold.0 > 31 {
            self.reply_to_nexus(
                request_id,
                NexusRspKind::Error(NexusRspError::InvalidThreshold(threshold)),
            );
            return Err(());
        }

        Ok(())
    }

    fn send_to_peer(&mut self, to: BaseboardId, msg: PeerMsg) {
        self.outgoing.push(Output::Envelope(Envelope {
            to,
            from: self.id.clone(),
            msg,
        }));
    }

    fn reply_to_nexus(&mut self, request_id: Uuid, rsp: NexusRspKind) {
        self.outgoing.push(Output::NexusRsp(NexusRsp {
            request_id,
            from: self.id.clone(),
            kind: rsp,
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

    fn persist_lrtq_cancelled(&mut self, lrtq_upgrade_id: Uuid) {
        self.outgoing.push(Output::PersistLrtqCancelled { lrtq_upgrade_id });
    }
}
