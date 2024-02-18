// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of the oxide rack trust quorum protocol

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Instant;
use uuid::Uuid;
use zeroize::ZeroizeOnDrop;

mod configuration;
mod messages;
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
    PersistLrtqCancelled { lrtq_upgrade_id: Uuid },
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct EncryptedRackSecret(pub Vec<u8>);
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    ZeroizeOnDrop,
)]

pub struct Share(Vec<u8>);

impl Share {
    pub fn new(share: Vec<u8>) -> Share {
        assert_eq!(share.len(), SHARE_SIZE);
        Share(share)
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

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct State {
    pub prepares: BTreeMap<Epoch, PrepareMsg>,
    pub commits: BTreeMap<Epoch, CommitMsg>,

    // Has the node seen a commit for an epoch higher than it's current
    // configuration for which it has not received a `PrepareMsg` for? If at
    // any time this gets set, than the it remains true for the lifetime of the
    // node. The sled corresponding to the node must be factory reset by wiping
    // its storage.
    pub decommissioned: Option<DecommissionedMetadata>,
}

impl State {
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
}

/// A node capable of participating in trust quorum
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Node {
    id: BaseboardId,
    state: State,
    outgoing: Vec<Output>,
}

impl Node {
    pub fn new(id: BaseboardId) -> Node {
        Node { id, state: State::default(), outgoing: Vec::new() }
    }

    pub fn id(&self) -> &BaseboardId {
        &self.id
    }

    pub fn handle_nexus_request(
        &mut self,
        NexusReq { id, kind }: NexusReq,
    ) -> impl Iterator<Item = Output> + '_ {
        match kind {
            NexusReqKind::Commit(msg) => todo!(),
            NexusReqKind::GetCommitted(epoch) => todo!(),
            NexusReqKind::GetLrtqShareHash => todo!(),
            NexusReqKind::UpgradeFromLrtq(msg) => {
                self.upgrade_from_lrtq(id, msg)
            }
            NexusReqKind::CancelUpgradeFromLrtq(msg) => {
                self.cancel_upgrade_from_lrtq(id, msg)
            }
        }
        self.outgoing.drain(..)
    }

    pub fn handle_peer_msg(
        &mut self,
        from: BaseboardId,
        msg: Msg,
    ) -> impl Iterator<Item = Output> + '_ {
        // TODO: Everything else
        self.outgoing.drain(..)
    }

    pub fn tick(&mut self, now: Instant) -> impl Iterator<Item = Output> + '_ {
        // TODO: Everything else
        self.outgoing.drain(..)
    }

    fn upgrade_from_lrtq(&mut self, request_id: Uuid, msg: UpgradeFromLrtqMsg) {
        if let Some(decommissioned) = &self.state.decommissioned {
            return self.reply_to_nexus(
                request_id,
                NexusRspKind::Error(NexusRspError::SledDecommissioned {
                    from: decommissioned.from.clone(),
                    epoch: decommissioned.epoch,
                    last_prepared_epoch: self.state.last_prepared_epoch(),
                }),
            );
        }

        if let Some(epoch) = self.state.last_prepared_epoch() {
            return self.reply_to_nexus(
                request_id,
                NexusRspKind::Error(NexusRspError::AlreadyPrepared(epoch)),
            );
        }

        if msg.members.len() <= msg.lrtq_ledger_data.threshold.0 as usize {
            return self.reply_to_nexus(
                request_id,
                NexusRspKind::Error(
                    NexusRspError::MembershipThresholdMismatch {
                        num_members: msg.members.len(),
                        threshold: msg.lrtq_ledger_data.threshold,
                    },
                ),
            );
        }

        if msg.members.len() < 3 || msg.members.len() > 32 {
            return self.reply_to_nexus(
                request_id,
                NexusRspKind::Error(NexusRspError::InvalidMembershipSize(
                    msg.members.len(),
                )),
            );
        }

        if msg.lrtq_ledger_data.threshold.0 < 2
            || msg.lrtq_ledger_data.threshold.0 > 31
        {
            return self.reply_to_nexus(
                request_id,
                NexusRspKind::Error(NexusRspError::InvalidThreshold(
                    msg.lrtq_ledger_data.threshold,
                )),
            );
        }

        // Create and persist a prepare, and ack to Nexus
        let config = Configuration {
            rack_uuid: msg.lrtq_ledger_data.rack_uuid,
            epoch: Epoch(0),
            last_committed_epoch: Epoch(0),
            coordinator: msg.members.keys().next().unwrap().clone(),
            members: msg.members,
            threshold: msg.lrtq_ledger_data.threshold,
            encrypted: None,
            lrtq_upgrade_id: Some(msg.upgrade_id),
        };

        let prepare = PrepareMsg { config, share: msg.lrtq_ledger_data.share };
        self.state.prepares.insert(Epoch(0), prepare.clone());
        self.persist_prepare(prepare);
        self.reply_to_nexus(
            request_id,
            NexusRspKind::UpgradeFromLrtqAck { upgrade_id: msg.upgrade_id },
        );
    }

    fn cancel_upgrade_from_lrtq(
        &mut self,
        request_id: Uuid,
        msg: CancelUpgradeFromLrtqMsg,
    ) {
        if let Some(decommissioned) = &self.state.decommissioned {
            return self.reply_to_nexus(
                request_id,
                NexusRspKind::Error(NexusRspError::SledDecommissioned {
                    from: decommissioned.from.clone(),
                    epoch: decommissioned.epoch,
                    last_prepared_epoch: self.state.last_prepared_epoch(),
                }),
            );
        }

        if let Some(epoch) = self.state.last_committed_epoch() {
            return self.reply_to_nexus(
                request_id,
                NexusRspKind::Error(NexusRspError::AlreadyCommitted(epoch)),
            );
        }

        if let Some(epoch) = self.state.last_prepared_epoch() {
            if epoch > Epoch(0) {
                return self.reply_to_nexus(
                    request_id,
                    NexusRspKind::Error(NexusRspError::AlreadyPrepared(epoch)),
                );
            }
        }

        if let Some(config) = self.state.configuration(Epoch(0)) {
            if config.lrtq_upgrade_id == Some(msg.upgrade_id) {
                // Success!
                self.state.prepares.remove(&Epoch(0));
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
    }

    fn send(&mut self, to: BaseboardId, msg: Msg) {
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
