// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A trust quorum node that implements the trust quorum protocol

use crate::{Configuration, Epoch, messages::*};
use crate::{
    Envelope, Error, PlatformId, Threshold, persistent_state::PersistentState,
};

use crate::crypto::{
    EncryptedShares, KeyShareEd25519, KeyShareGf256, RackSecret,
    ReconstructedRackSecret, ShareDigestEd25519, ShareDigestGf256,
};
use slog::{Logger, error, info, o, warn};
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

    /// Configuration that will get persisted inside a `Prepare` message in a
    /// `Node`s `PersistentState`, once it is possible to create the Prepare.
    configuration: Configuration,

    /// What is the coordinator currently doing
    op: CoordinatorOperation,

    /// When to resend prepare messages next
    retry_deadline: Instant,
}

impl CoordinatorState {
    pub fn new(
        now: Instant,
        reconfigure_msg: ReconfigureMsg,
        configuration: Configuration,
        op: CoordinatorOperation,
    ) -> CoordinatorState {
        // We always set the retry deadline to `now` so that we will send
        // prepares upon new construction. This field gets updated after
        // prepares are sent.
        let retry_deadline = now;
        CoordinatorState {
            start_time: now,
            reconfigure_msg,
            configuration,
            op,
            retry_deadline,
        }
    }
}

/// What should the coordinator be doing?
pub enum CoordinatorOperation {
    CollectShares {
        epoch: Epoch,
        members: BTreeMap<PlatformId, ShareDigestGf256>,
        shares: BTreeMap<PlatformId, KeyShareGf256>,
    },
    // Epoch is always 0
    CollectLrtqShares {
        members: BTreeMap<PlatformId, ShareDigestEd25519>,
        shares: BTreeMap<PlatformId, KeyShareEd25519>,
    },
    Prepare {
        /// The set of Prepares to send to each node
        prepares: BTreeMap<PlatformId, PrepareMsg>,

        /// Acknowledgements that the prepare has been received
        prepare_acks: BTreeSet<PlatformId>,
    },
}

/// An entity capable of participating in trust quorum
pub struct Node {
    log: Logger,

    /// The unique hardware ID of a sled
    platform_id: PlatformId,

    /// State that gets persistenly stored in ledgers
    persistent_state: PersistentState,

    /// In memory state for when this node is coordinating a reconfiguration
    coordinator_state: Option<CoordinatorState>,
}

impl Node {
    pub fn new(
        log: Logger,
        platform_id: PlatformId,
        persistent_state: PersistentState,
    ) -> Node {
        let id_str = format!("{platform_id:?}");
        let log =
            log.new(o!("component" => "trust-quorum", "platform_id" => id_str));
        Node { log, platform_id, persistent_state, coordinator_state: None }
    }

    pub fn platform_id(&self) -> &PlatformId {
        &self.platform_id
    }

    /// Start coordinating a reconfiguration
    ///
    /// On success, puts messages that need sending to other nodes in `outbox`
    /// and returns a `PersistentState` which the caller must write to disk.
    ///
    /// For upgrading from LRTQ, use `coordinate_upgrade_from_lrtq`
    pub fn coordinate_reconfiguration(
        &mut self,
        now: Instant,
        outbox: &mut Vec<Envelope>,
        msg: ReconfigureMsg,
    ) -> Result<Option<PersistentState>, Error> {
        self.check_in_service()?;
        self.validate_reconfigure_msg(&msg)?;
        let persistent_state = self.set_coordinator_state(now, msg)?;
        self.send_coordinator_msgs(now, outbox)?;
        Ok(persistent_state)
    }

    // Send any required messages as a reconfiguration coordinator
    //
    // This varies depending upon the current `CoordinatorState`.
    //
    // In some cases a `PrepareMsg` will be added locally to the
    // `PersistentState`, requiring persistence from the caller. In this case we
    // will return a copy of it.
    //
    // This method is "in progress" - allow unused parameters for now
    #[allow(unused)]
    fn send_coordinator_msgs(
        &mut self,
        now: Instant,
        outbox: &mut Vec<Envelope>,
    ) -> Result<(), Error> {
        // This function is going to be called unconditionally in `tick`
        // callbacks. In this case we may not actually be a coordinator. We just
        // ignore the call in that case.
        let Some(state) = &self.coordinator_state else {
            return Ok(());
        };

        match &state.op {
            CoordinatorOperation::CollectShares { epoch, members, shares } => {}
            CoordinatorOperation::CollectLrtqShares { members, shares } => {}
            CoordinatorOperation::Prepare { prepares, prepare_acks } => {
                for (platform_id, prepare) in prepares.clone().into_iter() {
                    outbox.push(Envelope {
                        to: platform_id,
                        from: self.platform_id.clone(),
                        msg: PeerMsg::Prepare(prepare),
                    });
                }
            }
        }

        Ok(())
    }

    fn set_coordinator_state(
        &mut self,
        now: Instant,
        msg: ReconfigureMsg,
    ) -> Result<Option<PersistentState>, Error> {
        // Are we already coordinating?
        if let Some(coordinator_state) = &self.coordinator_state {
            let current_epoch = coordinator_state.reconfigure_msg.epoch;
            if coordinator_state.reconfigure_msg.epoch > msg.epoch {
                warn!(
                    self.log,
                    "Reconfiguration in progress: rejecting stale attempt";
                    "current_epoch" => current_epoch.to_string(),
                    "msg_epoch" => msg.epoch.to_string()
                );
                return Err(Error::ReconfigurationInProgress {
                    current_epoch: coordinator_state.reconfigure_msg.epoch,
                    msg_epoch: msg.epoch,
                });
            }

            if coordinator_state.reconfigure_msg.epoch == msg.epoch {
                if coordinator_state.reconfigure_msg != msg {
                    error!(
                        self.log,
                        concat!(
                            "Reconfiguration in progress for same epoch, ",
                            "but messages differ");
                        "epoch" => msg.epoch.to_string(),
                    );
                    return Err(Error::MismatchedReconfigurationForSameEpoch(
                        msg.epoch,
                    ));
                }

                // Idempotent request
                return Ok(None);
            }

            info!(
                self.log,
                "Configuration being coordinated changed";
                "previous_epoch" => current_epoch.to_string(),
                "new_epoch" => msg.epoch.to_string()
            );
        }

        // Create a configuration for this epoch
        let (config, shares) =
            Configuration::new(self.platform_id.clone(), &msg)?;

        // How (and if) we collect the previous configuration's secrets depends
        // upon this node's persistent state.
        let (op, persistent_state) = if self.persistent_state.is_uninitialized()
        {
            let mut prepares = BTreeMap::new();
            for (platform_id, share) in shares.into_iter() {
                let prepare_msg = PrepareMsg { config: config.clone(), share };
                if platform_id == self.platform_id {
                    // Add the prepare to our `PersistentState`
                    self.persistent_state
                        .prepares
                        .insert(msg.epoch, prepare_msg);
                } else {
                    // Create a message that requires sending
                    prepares.insert(platform_id, prepare_msg);
                }
            }
            (
                CoordinatorOperation::Prepare {
                    prepares,
                    prepare_acks: BTreeSet::new(),
                },
                Some(self.persistent_state.clone()),
            )
        } else if self.persistent_state.is_last_committed_config_lrtq() {
            // We should never get here, as we must upgrade from a reconfig
            // which is checked earlier in this code path.
            return Err(Error::UpgradeFromLrtqRequired);
        } else {
            // Safety: We already validated the last committed configuration before getting here
            let config =
                self.persistent_state.last_committed_configuration().unwrap();
            (
                CoordinatorOperation::CollectShares {
                    epoch: config.epoch,
                    members: config.members.clone(),
                    shares: BTreeMap::new(),
                },
                None,
            )
        };

        self.coordinator_state =
            Some(CoordinatorState::new(now, msg, config, op));

        Ok(persistent_state)
    }

    fn validate_reconfigure_msg(
        &self,
        msg: &ReconfigureMsg,
    ) -> Result<(), Error> {
        if self.persistent_state.is_lrtq_only() {
            return Err(Error::UpgradeFromLrtqRequired);
        }
        Self::check_reconfigure_membership_sizes(msg)?;
        self.check_rack_id(msg)?;
        self.check_reconfigure_epoch(msg)?;

        Ok(())
    }

    /// RackId's must remain the same over the lifetime of the trust quorum
    fn check_rack_id(&self, msg: &ReconfigureMsg) -> Result<(), Error> {
        if let Some(rack_id) = self.persistent_state.rack_id() {
            if rack_id != msg.rack_id {
                return Err(Error::MismatchedRackId {
                    expected: rack_id,
                    got: msg.rack_id,
                });
            }
        }

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
