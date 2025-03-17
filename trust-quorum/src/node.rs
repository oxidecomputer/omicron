// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::messages::{CommitMsg, PeerMsg, PrepareMsg, Reconfigure};
use crate::{
    CoordinatorState, Envelope, Epoch, Error, Output, PersistentState,
    PlatformId, Threshold,
};
use std::time::Instant;

/// A node capable of participating in trust quorum
pub struct Node {
    id: PlatformId,
    persistent_state: PersistentState,
    outgoing: Vec<Output>,

    // The state of a coordinator performing a reconfiguration
    coordinator_state: Option<CoordinatorState>,
}

impl Node {
    pub fn new(id: PlatformId, persistent_state: PersistentState) -> Node {
        Node {
            id,
            persistent_state,
            outgoing: Vec::new(),
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
        self.validate_reconfigure_msg(&msg)?;
        self.set_coordinator_state(now, msg)?;

        // Start collecting shares for `last_committed_epoch` if there are any
        // self.collect_shares_as_coordinator();

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

    fn set_coordinator_state(
        &mut self,
        now: Instant,
        msg: Reconfigure,
    ) -> Result<(), Error> {
        if let Some(coordinator_state) = &self.coordinator_state {
            if coordinator_state.reconfigure.epoch > msg.epoch {
                // TODO: Log that we are rejecting a stale configuration
                return Err(Error::ReconfigurationInProgress {
                    current_epoch: coordinator_state.reconfigure.epoch,
                    msg_epoch: msg.epoch,
                });
            }

            // TODO: Log that we are updating our configuration state
        }

        self.coordinator_state = Some(CoordinatorState::new(
            now,
            msg,
            self.persistent_state.last_committed_reconfiguration().cloned(),
        ));

        Ok(())
    }

    fn validate_reconfigure_msg(&self, msg: &Reconfigure) -> Result<(), Error> {
        self.check_membership_constraints(msg.members.len(), msg.threshold)?;
        self.check_in_service()?;
        self.check_reconfigure_msg_epochs(msg)?;
        Ok(())
    }

    /// Ensure that epoch in the `Reconfigure` message is valid
    /// given the current state of the node.
    fn check_reconfigure_msg_epochs(
        &self,
        msg: &Reconfigure,
    ) -> Result<(), Error> {
        let last_committed_epoch = self.persistent_state.last_committed_epoch();
        // We can only reconfigure if the current epoch matches the last
        // committed epoch in the `Reconfigure` request.
        //
        // This provides us a total order over committed epochs.
        if msg.last_committed_epoch != last_committed_epoch {
            return Err(Error::LastCommittedEpochMismatch {
                node_epoch: last_committed_epoch,
                msg_epoch: msg.last_committed_epoch,
            });
        }

        // Refuse to reconfigure if we don't have any commits and we have lrtq
        // ledger data. This requires a call to `coordinate_upgrade_from_lrtq`
        // instead.
        if last_committed_epoch.is_none()
            && self.persistent_state.lrtq_ledger_data.is_some()
        {
            return Err(Error::CannotReconfigureLrtqNode);
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

        Ok(())
    }

    /// Verify that the node is not decommissioned
    fn check_in_service(&self) -> Result<(), Error> {
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
        &self,
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
