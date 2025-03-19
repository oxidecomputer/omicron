// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A trust quorum node that implements the trust quorum protocol

use crate::{
    Envelope, Epoch, Error, PlatformId, Threshold,
    persistent_state::PersistentState,
};
use crate::{
    KeyShareEd25519, KeyShareGf256, ReconstructedRackSecret, messages::*,
};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

/// The state of a reconfiguration coordinator.
///
/// A coordinator can be any trust quorum node that is a member of both the old
/// and new group. The coordinator is chosen by Nexus for a given epoch when a
/// trust quorum reconfiguration is triggered. Reconfiguration is only performed
/// when the control plane is up, as we use Nexus to persist prepares and ensure
/// commitment happens, even if the system crashes while committing. If a
/// rack crash (such as a power outage) occurs before nexus is informed of the
/// prepares, nexus will  skip the epoch and start a new reconfiguration. This
/// allows progress to always be made with a full linearization of epochs.
pub struct CoordinatorState {
    /// When the reconfiguration started
    start_time: Instant,

    /// A copy of the message used to start this reconfiguration
    reconfigure_msg: ReconfigureMsg,

    /// Collection of shares and/or reconstructed rack secret
    /// for the prior configuration if there is one.
    previous_config_secrets: Option<PreviousConfigSecrets>,

    /// The set of Prepares sent to each node
    prepares: BTreeMap<PlatformId, PrepareMsg>,

    /// Acknowledgements that the prepare has been received
    prepare_acks: BTreeSet<PlatformId>,

    /// When to resend prepare messages next
    retry_deadline: Instant,
}

impl CoordinatorState {
    pub fn new(
        now: Instant,
        reconfigure_msg: ReconfigureMsg,
        previous_config_secrets: Option<PreviousConfigSecrets>,
    ) -> CoordinatorState {
        // We always set the retry deadline to `now` so that we will send
        // prepares upon new construction. This field gets updated after
        // prepares are sent.
        let retry_deadline = now;
        CoordinatorState {
            start_time: now,
            reconfigure_msg,
            previous_config_secrets,
            prepares: BTreeMap::new(),
            prepare_acks: BTreeSet::new(),
            retry_deadline,
        }
    }
}

/// Coordinator's collection of prior secret information
///
/// Used in all but the initial configuration.
pub enum PreviousConfigSecrets {
    /// If collecting shares for LRTQ, we start in this variant
    Ed25519Shares(BTreeMap<PlatformId, KeyShareEd25519>),
    /// If collecting shares for this protocol we start here
    Gf256Shares(BTreeMap<PlatformId, KeyShareGf256>),
    /// We end here when we have enough shares to reconstruct the rack secret
    RackSecret(ReconstructedRackSecret),
}

/// An entity capable of participating in trust quorum
pub struct Node {
    /// The unique hardware ID of a sled
    id: PlatformId,

    /// State that gets persistenly stored in ledgers
    persistent_state: PersistentState,

    /// In memory state for when this node is coordinating a reconfiguration
    coordinator_state: Option<CoordinatorState>,
}

impl Node {
    pub fn id(&self) -> &PlatformId {
        &self.id
    }

    /// Start coordinating a reconfiguration
    ///
    /// On success, puts messages that need sending to other nodes in `outbox`
    /// and returns a `PersistentState` which the caller must write to disk.
    pub fn coordinate_reconfiguration(
        &mut self,
        now: Instant,
        outbox: &mut Vec<Envelope>,
        msg: ReconfigureMsg,
    ) -> Result<Option<PersistentState>, Error> {
        self.check_in_service()?;
        self.validate_reconfigure_msg(&msg)?;
        self.set_coordinator_state(now, msg)?;
        self.send_coordinator_msgs(now, outbox)
    }

    // Send any required messages as a reconfiguration coordinator
    //
    // This varies depending upon the current `CoordinatorState`.
    //
    // In some cases a `PrepareMsg` will be added locally to the
    // `PersistentState`, requiring persistence from the caller. In this case we
    // will return a copy of it.
    fn send_coordinator_msgs(
        &mut self,
        now: Instant,
        outbox: &mut Vec<Envelope>,
    ) -> Result<Option<PersistentState>, Error> {
        todo!()
    }

    fn set_coordinator_state(
        &mut self,
        now: Instant,
        msg: ReconfigureMsg,
    ) -> Result<(), Error> {
        // Are we already coordinating?
        if let Some(coordinator_state) = &self.coordinator_state {
            if coordinator_state.reconfigure_msg.epoch > msg.epoch {
                // TODO: Log that we are rejecting a stale configuration
                return Err(Error::ReconfigurationInProgress {
                    current_epoch: coordinator_state.reconfigure_msg.epoch,
                    msg_epoch: msg.epoch,
                });
            }

            if coordinator_state.reconfigure_msg.epoch == msg.epoch {
                if coordinator_state.reconfigure_msg != msg {
                    // TODO: Log error about invalid message
                    return Err(Error::MismatchedReconfigurationForSameEpoch(
                        msg.epoch,
                    ));
                }

                // Idempotent request
                return Ok(());
            }

            // TODO: Log that we are updating our configuration state
        }

        // How we collect the previous configuration's secrets depends upon
        // this node's persistent state.
        let previous_config_secrets =
            if self.persistent_state.is_uninitialized() {
                None
            } else if self.persistent_state.is_last_committed_config_lrtq() {
                Some(PreviousConfigSecrets::Ed25519Shares(BTreeMap::new()))
            } else {
                Some(PreviousConfigSecrets::Gf256Shares(BTreeMap::new()))
            };

        self.coordinator_state =
            Some(CoordinatorState::new(now, msg, previous_config_secrets));

        Ok(())
    }

    fn validate_reconfigure_msg(
        &self,
        msg: &ReconfigureMsg,
    ) -> Result<(), Error> {
        Self::check_reconfigure_membership_sizes(msg)?;
        self.check_reconfigure_epoch(msg)?;

        Ok(())
    }

    fn check_reconfigure_epoch(
        &self,
        msg: &ReconfigureMsg,
    ) -> Result<(), Error> {
        // Ensure we are strictly ordering committed configurations
        if msg.last_committed_epoch
            != self.persistent_state.last_committed_epoch()
        {
            return Err(Error::LastCommittedEpochMismatch {
                node_epoch: self.persistent_state.last_committed_epoch(),
                msg_epoch: msg.last_committed_epoch,
            });
        }

        // Ensure that we haven't seen a prepare message for a newer
        // configuration.
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
    /// constraints.
    fn check_reconfigure_membership_sizes(
        msg: &ReconfigureMsg,
    ) -> Result<(), Error> {
        let num_members = msg.members.len();
        if num_members <= msg.threshold.0 as usize {
            return Err(Error::MembershipThresholdMismatch {
                num_members,
                threshold: msg.threshold,
            });
        }

        if num_members < 3 || num_members > 32 {
            return Err(Error::InvalidMembershipSize(num_members));
        }

        if msg.threshold.0 < 2 || msg.threshold.0 > 31 {
            return Err(Error::InvalidThreshold(msg.threshold));
        }

        Ok(())
    }
}
