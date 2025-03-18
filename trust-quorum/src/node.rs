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
    ) -> Result<PersistentState, Error> {
        self.check_in_service()?;
        self.validate_reconfigure_msg(&msg)?;
    }

    fn validate_reconfigure_msg(
        &self,
        msg: &ReconfigureMsg,
    ) -> Result<(), Error> {
        Self::check_membership_sizes(msg)?;

        if let Some(last_committed_epoch) = msg.last_committed_epoch {
            // We can't commit a reconfiguration if this node isn't part of the
            // old group.
            if self.persistent_state.is_uninitialized() {
                return Err(Error::LastCommittedEpochMismatch {
                    node_epoch: None,
                    msg_epoch: Some(last_committed_epoch),
                });
            }

            // We assume LRTQ is epoch 0
            if self.persistent_state.is_lrtq_only()
                && last_committed_epoch != Epoch(0)
            {
                return Err(Error::LastCommittedEpochMismatch {
                    node_epoch: Some(Epoch(0)),
                    msg_epoch: Some(last_committed_epoch),
                });
            }
        } else {
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
    fn check_membership_sizes(msg: &ReconfigureMsg) -> Result<(), Error> {
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
