// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! State of a reconfiguration coordinator inside a [`crate::Node`]

use crate::configuration::PreviousConfiguration;
use crate::crypto::{self, LrtqShare, Sha3_256Digest, ShareDigestLrtq};
use crate::errors::ReconfigurationError;
use crate::messages::{PeerMsg, PrepareMsg};
use crate::validators::ValidatedReconfigureMsg;
use crate::{
    Configuration, Envelope, Epoch, PlatformId, RackSecret, Threshold,
};
use gfss::shamir::Share;
use slog::{Logger, error, info, o, warn};
use std::collections::{BTreeMap, BTreeSet};
use std::mem;
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
///
/// We allow some unused fields before we complete the coordination code
pub struct CoordinatorState {
    log: Logger,

    /// When the reconfiguration started
    #[expect(unused)]
    start_time: Instant,

    /// A copy of the message used to start this reconfiguration
    reconfigure_msg: ValidatedReconfigureMsg,

    /// Configuration that will get persisted inside a `Prepare` message in a
    /// `Node`s `PersistentState`, once it is possible to create the Prepare.
    configuration: Configuration,

    /// What is the coordinator currently doing
    op: CoordinatorOperation,

    /// When to resend prepare messages next
    retry_deadline: Instant,
}

impl CoordinatorState {
    /// Start coordinating a reconfiguration for a brand new trust quorum
    ///
    /// Return the newly constructed `CoordinatorState` along with this node's
    /// `PrepareMsg` so that it can be persisted.
    pub fn new_uninitialized(
        log: Logger,
        now: Instant,
        msg: ValidatedReconfigureMsg,
    ) -> Result<(CoordinatorState, PrepareMsg), ReconfigurationError> {
        // Create a configuration for this epoch
        let (config, shares) = Configuration::new(&msg)?;

        let mut prepares = BTreeMap::new();
        // `my_prepare_msg` is optional only so that we can fill it in via
        // the loop. It will always become `Some`, as a `Configuration` always
        // contains the coordinator as a member as validated by construction of
        // `ValidatedReconfigureMsg`.
        let mut my_prepare_msg: Option<PrepareMsg> = None;
        for (platform_id, share) in shares.into_iter() {
            let prepare_msg = PrepareMsg { config: config.clone(), share };
            if platform_id == *msg.coordinator_id() {
                // The prepare message to add to our `PersistentState`
                my_prepare_msg = Some(prepare_msg);
            } else {
                // Create a message that requires sending
                prepares.insert(platform_id, prepare_msg);
            }
        }
        let op = CoordinatorOperation::Prepare {
            prepares,
            // Always include ourself
            prepare_acks: BTreeSet::from([msg.coordinator_id().clone()]),
        };

        info!(
            log,
            "Starting coordination on uninitialized node";
            "epoch" => %config.epoch
        );

        let state = CoordinatorState::new(log, now, msg, config, op);

        // Safety: Construction of a `ValidatedReconfigureMsg` ensures that
        // `my_platform_id` is part of the new configuration and has a share.
        // We can therefore safely unwrap here.
        Ok((state, my_prepare_msg.unwrap()))
    }

    /// A reconfiguration from one group to another
    pub fn new_reconfiguration(
        log: Logger,
        now: Instant,
        msg: ValidatedReconfigureMsg,
        last_committed_config: &Configuration,
        our_last_committed_share: Share,
    ) -> Result<CoordinatorState, ReconfigurationError> {
        let (config, new_shares) = Configuration::new(&msg)?;

        info!(
            log,
            "Starting coordination on existing node";
            "epoch" => %config.epoch,
            "last_committed_epoch" => %last_committed_config.epoch
        );

        // We must collect shares from the last configuration
        // so we can recompute the old rack secret.
        let op = CoordinatorOperation::CollectShares {
            last_committed_epoch: last_committed_config.epoch,
            last_committed_members: last_committed_config.members.clone(),
            last_committed_threshold: last_committed_config.threshold,
            // Always include ourself
            collected_shares: BTreeMap::from([(
                msg.coordinator_id().clone(),
                our_last_committed_share,
            )]),
            new_shares,
        };

        Ok(CoordinatorState::new(log, now, msg, config, op))
    }

    // Intentionally private!
    //
    // The public constructors `new_uninitialized` and `new_reconfiguration` are
    // more specific, and perform validation of arguments.
    fn new(
        log: Logger,
        now: Instant,
        reconfigure_msg: ValidatedReconfigureMsg,
        configuration: Configuration,
        op: CoordinatorOperation,
    ) -> CoordinatorState {
        // We want to send any pending messages immediately
        let retry_deadline = now;
        CoordinatorState {
            log: log.new(o!("component" => "tq-coordinator-state")),
            start_time: now,
            reconfigure_msg,
            configuration,
            op,
            retry_deadline,
        }
    }

    // Return the `ValidatedReconfigureMsg` that started this reconfiguration
    pub fn reconfigure_msg(&self) -> &ValidatedReconfigureMsg {
        &self.reconfigure_msg
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
    pub fn send_msgs(&mut self, now: Instant, outbox: &mut Vec<Envelope>) {
        if now < self.retry_deadline {
            return;
        }
        self.retry_deadline = now + self.reconfigure_msg.retry_timeout();
        match &self.op {
            CoordinatorOperation::CollectShares {
                last_committed_epoch,
                last_committed_members,
                collected_shares,
                ..
            } => {
                // Send to all members that we haven't yet collected shares from.
                // Also exclude ourself.
                for member in last_committed_members
                    .keys()
                    .filter(|&m| !collected_shares.contains_key(m))
                    .cloned()
                {
                    outbox.push(Envelope {
                        to: member,
                        from: self.reconfigure_msg.coordinator_id().clone(),
                        msg: PeerMsg::GetShare(*last_committed_epoch),
                    });
                }
            }
            CoordinatorOperation::CollectLrtqShares { .. } => {}
            CoordinatorOperation::Prepare { prepares, .. } => {
                // prepares already filter ourself
                for (platform_id, prepare) in prepares.clone().into_iter() {
                    outbox.push(Envelope {
                        to: platform_id,
                        from: self.reconfigure_msg.coordinator_id().clone(),
                        msg: PeerMsg::Prepare(prepare),
                    });
                }
            }
        }
    }

    /// Record a `PrepareAck` from another node as part of tracking
    /// quorum for the prepare phase of the trust quorum protocol.
    pub fn ack_prepare(&mut self, from: PlatformId) {
        match &mut self.op {
            CoordinatorOperation::Prepare {
                prepares, prepare_acks, ..
            } => {
                if !self.configuration.members.contains_key(&from) {
                    warn!(
                        self.log,
                        "PrepareAck from node that is not a cluster member";
                        "epoch" => %self.configuration.epoch,
                        "from" => %from
                    );
                    return;
                }

                // Remove the responder so we don't ask it again
                prepares.remove(&from);

                // Save the ack for quorum purposes
                prepare_acks.insert(from);
            }
            op => {
                warn!(
                    self.log,
                    "Ack received when coordinator is not preparing";
                    "op" => op.name(),
                    "from" => %from
                );
            }
        }
    }

    /// Handle a share response for the last_committed epoch.
    ///
    /// If we transition from collecting shares to sending prepare messages,
    /// we also return our own `PrepareMsg` that must be saved as part of the
    /// persistent state.
    pub fn handle_share(
        &mut self,
        now: Instant,
        outbox: &mut Vec<Envelope>,
        from: PlatformId,
        epoch: Epoch,
        share: Share,
    ) -> Option<PrepareMsg> {
        match &mut self.op {
            CoordinatorOperation::CollectShares {
                last_committed_epoch,
                last_committed_members,
                last_committed_threshold,
                collected_shares,
                new_shares,
            } => {
                // First, perform some validation on the incoming share
                if *last_committed_epoch != epoch {
                    warn!(
                        self.log,
                        "Received Share from node with wrong epoch";
                        "epoch" => %epoch,
                        "from" => %from
                    );
                    return None;
                }

                let Some(expected_digest) = last_committed_members.get(&from)
                else {
                    warn!(
                        self.log,
                        "Received Share from unexpected node";
                        "epoch" => %epoch,
                        "from" => %from
                    );
                    return None;
                };

                let mut digest = Sha3_256Digest::default();
                share.digest::<sha3::Sha3_256>(&mut digest.0);
                if digest != *expected_digest {
                    error!(
                        self.log,
                        "Received share with invalid digest";
                        "epoch" => %epoch,
                        "from" => %from
                    );
                }

                // A valid share was received. Is it new?
                if collected_shares.insert(from, share).is_some() {
                    return None;
                }
                //
                // Do we have enough shares to recompute the rack secret
                // for `epoch`?
                if collected_shares.len() < last_committed_threshold.0 as usize
                {
                    return None;
                }

                // Reconstruct the old rack secret from the shares we collected.
                let old_rack_secret = match RackSecret::reconstruct_from_iter(
                    collected_shares.values(),
                ) {
                    Ok(old_rack_secret) => {
                        info!(
                            self.log,
                            "Successfully reconstructed old rack secret";
                            "last_committed_epoch" => %epoch,
                            "epoch" => %self.configuration.epoch
                        );

                        old_rack_secret
                    }
                    Err(err) => {
                        error!(
                            self.log,
                            "Failed to reconstruct old rack secret: {err}";
                            "epoch" => %epoch
                        );
                        return None;
                    }
                };

                // Reconstruct the new rack secret from the new shares.
                let new_rack_secret = match RackSecret::reconstruct_from_iter(
                    new_shares.values(),
                ) {
                    Ok(new_rack_secret) => {
                        info!(
                            self.log,
                            "Successfully reconstructed new rack secret";
                            "last_committed_epoch" => %epoch,
                            "epoch" => %self.configuration.epoch
                        );
                        new_rack_secret
                    }
                    Err(err) => {
                        error!(
                            self.log,
                            "Failed to reconstruct new rack secret: {err}";
                            "epoch" => %epoch
                        );
                        return None;
                    }
                };

                // Encrypt our old secret with a key derived from the new secret
                let (encrypted_last_committed_rack_secret, salt) =
                    match crypto::encrypt_old_rack_secret(
                        old_rack_secret,
                        new_rack_secret,
                        self.configuration.rack_id,
                        *last_committed_epoch,
                        self.configuration.epoch,
                    ) {
                        Ok(val) => val,
                        Err(_) => {
                            error!(
                                self.log, "Failed to encrypt old rack secret";
                                "last_committed_epoch" => %epoch,
                                "epoch" => %self.configuration.epoch
                            );
                            return None;
                        }
                    };

                // Create and set our previous configuration
                assert!(self.configuration.previous_configuration.is_none());
                let previous_config = PreviousConfiguration {
                    epoch: *last_committed_epoch,
                    is_lrtq: false,
                    encrypted_last_committed_rack_secret,
                    encrypted_last_committed_rack_secret_salt: salt,
                };
                self.configuration.previous_configuration =
                    Some(previous_config);

                // Transition to sending `PrepareMsg`s for this configuration
                let my_prepare_msg =
                    self.start_preparing_after_collecting_shares(now, outbox);
                Some(my_prepare_msg)
            }
            op => {
                warn!(
                    self.log,
                    "Share received when coordinator is not expecting it";
                    "op" => op.name(),
                    "from" => %from
                );
                None
            }
        }
    }

    pub fn coordinator_status(&mut self) -> CoordinatorStatus {
        (&self.op).into()
    }

    // Transition from `CoordinationOperation::CollectShares`
    // or `CoordinationOperation::CollectLrtqShares` to
    // `CoordinationOperation::Prepare`.
    //
    // Return our own prepare message so it can be persisted.
    //
    // Panics if the current op is already `CoordinationOperation::Prepare`.
    fn start_preparing_after_collecting_shares(
        &mut self,
        now: Instant,
        outbox: &mut Vec<Envelope>,
    ) -> PrepareMsg {
        // Get the set of members in both the old and new group along with the
        // shares mapped to all members in the new group.
        let (existing_members, new_shares) = match &mut self.op {
            CoordinatorOperation::CollectShares {
                last_committed_members,
                new_shares,
                ..
            } => {
                let existing_members: BTreeSet<_> =
                    mem::take(last_committed_members).into_keys().collect();
                (existing_members, mem::take(new_shares))
            }
            CoordinatorOperation::CollectLrtqShares {
                last_committed_members,
                new_shares,
                ..
            } => {
                let existing_members: BTreeSet<_> =
                    mem::take(last_committed_members).into_keys().collect();
                (existing_members, mem::take(new_shares))
            }
            CoordinatorOperation::Prepare { .. } => {
                error!(
                    self.log,
                    "logic error: already preparing";
                    "epoch" => %self.configuration.epoch,
                );
                panic!(
                    "logic error: already preparing: epoch = {}",
                    self.configuration.epoch
                );
            }
        };

        // Build up our set of `PrepareMsgs`
        //
        // `my_prepare_msg` is optional only so that we can fill it in via
        // the loop. It will always become `Some`, as a `Configuration` always
        // contains the coordinator as a member as validated by construction of
        // `ValidatedReconfigureMsg`.
        let mut my_prepare_msg: Option<PrepareMsg> = None;
        let mut prepares = BTreeMap::new();
        for (member, share) in new_shares {
            if existing_members.contains(&member) {
                let prepare_msg =
                    PrepareMsg { config: self.configuration.clone(), share };
                if *self.reconfigure_msg.coordinator_id() == member {
                    my_prepare_msg = Some(prepare_msg);
                } else {
                    prepares.insert(member, prepare_msg);
                }
            } else {
                // New members do not get sent information about the previous
                // configuration.
                let mut config = self.configuration.clone();
                config.previous_configuration = None;
                let prepare_msg = PrepareMsg { config, share };
                prepares.insert(member, prepare_msg);
            }
        }

        // Actually transition to the new operation
        self.op = CoordinatorOperation::Prepare {
            prepares,
            // Always include ourself
            prepare_acks: BTreeSet::from([self
                .reconfigure_msg
                .coordinator_id()
                .clone()]),
        };

        let last_committed_epoch =
            self.configuration.previous_configuration.as_ref().unwrap().epoch;
        info!(
            self.log,
            "Starting to prepare after collecting shares";
            "epoch" => %self.configuration.epoch,
            // Safety: This whole method relies on having a previous configuration
            "last_committed_epoch" => %last_committed_epoch
        );

        // Trigger sending of Prepare messages immediately
        self.retry_deadline = now;
        self.send_msgs(now, outbox);

        // Return our own `PrepareMsg` for persistence
        // Safety: Construction of a `ValidatedReconfigureMsg` ensures that
        // `my_platform_id` is part of the new configuration and has a share.
        // We can therefore safely unwrap here.
        my_prepare_msg.unwrap()
    }
}

/// What should the coordinator be doing?
pub enum CoordinatorOperation {
    // We haven't started implementing this yet
    CollectShares {
        last_committed_epoch: Epoch,
        last_committed_members: BTreeMap<PlatformId, Sha3_256Digest>,
        last_committed_threshold: Threshold,

        // Shares collected from `last_committed_members`
        collected_shares: BTreeMap<PlatformId, Share>,

        // New shares to be used when we get to the `Prepare` operation
        new_shares: BTreeMap<PlatformId, Share>,
    },
    // We haven't started implementing this yet
    // Epoch is always 0
    CollectLrtqShares {
        last_committed_members: BTreeMap<PlatformId, ShareDigestLrtq>,
        last_committed_threshold: Threshold,
        collected_shares: BTreeMap<PlatformId, LrtqShare>,

        // New shares to be used when we get to the `Prepare` operation
        new_shares: BTreeMap<PlatformId, Share>,
    },
    Prepare {
        /// The set of Prepares to send to each node
        prepares: BTreeMap<PlatformId, PrepareMsg>,

        /// Acknowledgements that the prepare has been received
        prepare_acks: BTreeSet<PlatformId>,
    },
}

impl CoordinatorOperation {
    pub fn name(&self) -> &'static str {
        match self {
            CoordinatorOperation::CollectShares { .. } => "collect shares",
            CoordinatorOperation::CollectLrtqShares { .. } => {
                "collect lrtq shares"
            }
            CoordinatorOperation::Prepare { .. } => "prepare",
        }
    }
}

/// A summary of the coordinator's current operational status
pub enum CoordinatorStatus {
    CollectShares { collected_from: BTreeSet<PlatformId> },
    CollectLrtqShares { collected_from: BTreeSet<PlatformId> },
    Prepare { acked: BTreeSet<PlatformId> },
}

impl From<&CoordinatorOperation> for CoordinatorStatus {
    fn from(value: &CoordinatorOperation) -> Self {
        match value {
            CoordinatorOperation::CollectShares {
                collected_shares, ..
            } => CoordinatorStatus::CollectShares {
                collected_from: collected_shares.keys().cloned().collect(),
            },
            CoordinatorOperation::CollectLrtqShares {
                collected_shares,
                ..
            } => CoordinatorStatus::CollectLrtqShares {
                collected_from: collected_shares.keys().cloned().collect(),
            },
            CoordinatorOperation::Prepare { prepare_acks, .. } => {
                CoordinatorStatus::Prepare { acked: prepare_acks.clone() }
            }
        }
    }
}
