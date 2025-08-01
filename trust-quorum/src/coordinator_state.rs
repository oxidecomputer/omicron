// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! State of a reconfiguration coordinator inside a [`crate::Node`]

use crate::NodeHandlerCtx;
use crate::crypto::{
    LrtqShare, PlaintextRackSecrets, Sha3_256Digest, ShareDigestLrtq,
};
use crate::validators::{ReconfigurationError, ValidatedReconfigureMsg};
use crate::{Configuration, Epoch, PeerMsgKind, PlatformId, RackSecret};
use gfss::shamir::Share;
use slog::{Logger, error, info, o, warn};
use std::collections::{BTreeMap, BTreeSet};
use std::mem;

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

    /// A copy of the message used to start this reconfiguration
    reconfigure_msg: ValidatedReconfigureMsg,

    /// Configuration that will get persisted inside a `Prepare` message in a
    /// `Node`s `PersistentState`, once it is possible to create the Prepare.
    configuration: Configuration,

    /// What is the coordinator currently doing
    op: CoordinatorOperation,
}

impl CoordinatorState {
    /// Start coordinating a reconfiguration for a brand new trust quorum
    ///
    /// Return the newly constructed `CoordinatorState` along with this node's
    /// `PrepareMsg` so that it can be persisted.
    pub fn new_uninitialized(
        log: Logger,
        msg: ValidatedReconfigureMsg,
    ) -> Result<(CoordinatorState, Configuration, Share), ReconfigurationError>
    {
        // Create a configuration for this epoch
        let (config, shares) = Configuration::new(&msg)?;

        let mut prepares = BTreeMap::new();
        // `my_share` is optional only so that we can fill it in via the
        // loop. It will always become `Some`, as a `Configuration` always
        // contains the coordinator as a member as validated by construction of
        // `ValidatedReconfigureMsg`.
        let mut my_share: Option<Share> = None;
        for (platform_id, share) in shares.into_iter() {
            if platform_id == *msg.coordinator_id() {
                // The data to add to our `PersistentState`
                my_share = Some(share);
            } else {
                // Create a message that requires sending
                prepares.insert(platform_id, (config.clone(), share));
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

        let state = CoordinatorState::new(log, msg, config.clone(), op);

        // Safety: Construction of a `ValidatedReconfigureMsg` ensures that
        // `my_platform_id` is part of the new configuration and has a share.
        // We can therefore safely unwrap here.
        Ok((state, config, my_share.unwrap()))
    }

    /// A reconfiguration from one group to another
    pub fn new_reconfiguration(
        log: Logger,
        msg: ValidatedReconfigureMsg,
        latest_committed_config: &Configuration,
        our_latest_committed_share: Share,
    ) -> Result<CoordinatorState, ReconfigurationError> {
        let (config, new_shares) = Configuration::new(&msg)?;

        info!(
            log,
            "Starting coordination on existing node";
            "epoch" => %config.epoch,
            "last_committed_epoch" => %latest_committed_config.epoch
        );

        // We must collect shares from the last committed configuration so we
        // can recompute the old rack secret.
        let op = CoordinatorOperation::CollectShares {
            // We save this so we can grab the old configuration
            old_epoch: latest_committed_config.epoch,
            // Always include ourself
            old_collected_shares: BTreeMap::from([(
                msg.coordinator_id().clone(),
                our_latest_committed_share,
            )]),
            new_shares,
        };

        Ok(CoordinatorState::new(log, msg, config, op))
    }

    // Intentionally private!
    //
    // The public constructors `new_uninitialized` and `new_reconfiguration` are
    // more specific, and perform validation of arguments.
    fn new(
        log: Logger,
        reconfigure_msg: ValidatedReconfigureMsg,
        configuration: Configuration,
        op: CoordinatorOperation,
    ) -> CoordinatorState {
        CoordinatorState {
            log: log.new(o!("component" => "tq-coordinator-state")),
            reconfigure_msg,
            configuration,
            op,
        }
    }

    /// Return the `ValidatedReconfigureMsg` that started this reconfiguration
    pub fn reconfigure_msg(&self) -> &ValidatedReconfigureMsg {
        &self.reconfigure_msg
    }

    pub fn op(&self) -> &CoordinatorOperation {
        &self.op
    }

    /// Send any required messages as a reconfiguration coordinator
    ///
    /// This varies depending upon the current `CoordinatorState`.
    pub fn send_msgs(&mut self, ctx: &mut impl NodeHandlerCtx) {
        match &self.op {
            CoordinatorOperation::CollectShares {
                old_epoch,
                old_collected_shares,
                ..
            } => {
                // Send to all connected members in the last committed
                // configuration that we haven't yet collected shares from.
                let destinations: Vec<_> = ctx
                    .persistent_state()
                    .configuration(*old_epoch)
                    .expect("config exists")
                    .members
                    .keys()
                    .filter(|&m| {
                        !old_collected_shares.contains_key(m)
                            && ctx.connected().contains(m)
                    })
                    .cloned()
                    .collect();
                for to in destinations {
                    ctx.send(to, PeerMsgKind::GetShare(*old_epoch));
                }
            }
            #[expect(unused)]
            CoordinatorOperation::CollectLrtqShares { members, shares } => {}
            CoordinatorOperation::Prepare { prepares, .. } => {
                for (platform_id, (config, share)) in
                    prepares.clone().into_iter()
                {
                    if ctx.connected().contains(&platform_id) {
                        ctx.send(
                            platform_id,
                            PeerMsgKind::Prepare { config, share },
                        );
                    }
                }
            }
        }
    }

    // Send any required messages to a newly connected node
    // This method is "in progress" - allow unused parameters for now
    #[expect(unused)]
    pub fn send_msgs_to(
        &mut self,
        ctx: &mut impl NodeHandlerCtx,
        to: PlatformId,
    ) {
        match &self.op {
            CoordinatorOperation::CollectShares {
                old_epoch,
                old_collected_shares,
                ..
            } => {}
            CoordinatorOperation::CollectLrtqShares { members, shares } => {}
            CoordinatorOperation::Prepare { prepares, prepare_acks } => {
                let rack_id = self.reconfigure_msg.rack_id();
                if let Some((config, share)) = prepares.get(&to) {
                    ctx.send(
                        to,
                        PeerMsgKind::Prepare {
                            config: config.clone(),
                            share: share.clone(),
                        },
                    );
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

    pub fn handle_share(
        &mut self,
        ctx: &mut impl NodeHandlerCtx,
        from: PlatformId,
        epoch: Epoch,
        share: Share,
    ) {
        match &mut self.op {
            CoordinatorOperation::CollectShares {
                old_epoch,
                old_collected_shares,
                new_shares,
            } => {
                // SAFETY: We started coordinating by looking up the last
                // committed configuration, which gave us `old_epoch`. Therefore
                // the configuration must exist.
                let old_config = ctx
                    .persistent_state()
                    .configuration(*old_epoch)
                    .expect("config exists");

                let new_epoch = self.configuration.epoch;

                let log = self.log.new(o!(
                    "last_committed_epoch" => old_epoch.to_string(),
                    "new_epoch" => new_epoch.to_string()
                ));

                // Are we trying to retrieve shares for `epoch`?
                if *old_epoch != epoch {
                    warn!(
                        log,
                        "Received Share from node with wrong epoch";
                        "received_epoch" => %epoch,
                        "from" => %from
                    );
                    return;
                }

                // Was the sender a member of the configuration at `old_epoch`?
                let Some(expected_digest) = old_config.members.get(&from)
                else {
                    warn!(
                        log,
                        "Received Share from unexpected node";
                        "received_epoch" => %epoch,
                        "from" => %from
                    );
                    return;
                };

                // Does the share hash match what we expect?
                let mut digest = Sha3_256Digest::default();
                share.digest::<sha3::Sha3_256>(&mut digest.0);
                if digest != *expected_digest {
                    error!(
                        log,
                        "Received share with invalid digest";
                        "received_epoch" => %epoch,
                        "from" => %from
                    );
                }

                // A valid share was received. Is it new?
                if old_collected_shares.insert(from, share).is_some() {
                    return;
                }

                // Do we have enough shares to recompute the old rack secret?
                if old_collected_shares.len() < old_config.threshold.0 as usize
                {
                    return;
                }

                // Reconstruct the old rack secret from the shares we collected.
                let shares: Vec<_> =
                    old_collected_shares.values().cloned().collect();
                let old_rack_secret = match RackSecret::reconstruct(&shares) {
                    Ok(secret) => {
                        info!(
                            log,
                            "Successfully reconstructed old rack secret"
                        );
                        secret
                    }
                    Err(err) => {
                        error!(
                            log,
                            "Failed to reconstruct old rack secret";
                            &err
                        );
                        return;
                    }
                };

                // Reconstruct the new rack secret from the shares we created
                // at coordination start time.
                let shares: Vec<_> = new_shares.values().cloned().collect();
                let new_rack_secret = match RackSecret::reconstruct(&shares) {
                    Ok(secret) => {
                        info!(
                            log,
                            "Successfully reconstructed new rack secret"
                        );
                        secret
                    }
                    Err(err) => {
                        error!(
                            log,
                            "Failed to reconstruct new rack secret";
                            &err
                        );
                        return;
                    }
                };

                // Decrypt the encrypted rack secrets from the old config so
                // that we can add `old_rack_secret` to that set for use in the
                // new configuration.
                let mut plaintext_secrets = if let Some(encrypted_secrets) =
                    &old_config.encrypted_rack_secrets
                {
                    match encrypted_secrets.decrypt(
                        old_config.rack_id,
                        old_config.epoch,
                        &old_rack_secret,
                    ) {
                        Ok(plaintext) => plaintext,
                        Err(err) => {
                            error!(log, "Rack secrets decryption error"; &err);
                            return;
                        }
                    }
                } else {
                    PlaintextRackSecrets::new()
                };
                plaintext_secrets.insert(*old_epoch, old_rack_secret);

                // Now encrypt the set of old rack secrets with the new rack
                // secret.
                let new_encrypted_rack_secrets = match plaintext_secrets
                    .encrypt(
                        self.configuration.rack_id,
                        new_epoch,
                        &new_rack_secret,
                    ) {
                    Ok(ciphertext) => ciphertext,
                    Err(_) => {
                        error!(log, "Failed to encrypt plaintext rack secrets");
                        return;
                    }
                };

                // Save the encrypted rack secrets in the current configuration
                assert!(self.configuration.encrypted_rack_secrets.is_none());
                self.configuration.encrypted_rack_secrets =
                    Some(new_encrypted_rack_secrets);

                // Take `new_shares` out of `self.op` so we can include them in
                // `Prepare` messages;
                let mut new_shares = mem::take(new_shares);

                // Update our persistent state
                //
                // We remove ourself because we don't send a `Prepare` message
                // to ourself.
                //
                // SAFETY: our share already exists at this point and has been
                // validated as part of the `Configuration` construction.
                let share = new_shares
                    .remove(ctx.platform_id())
                    .expect("my share exists");
                ctx.update_persistent_state(|ps| {
                    ps.shares.insert(new_epoch, share);
                    ps.configs
                        .insert_unique(self.configuration.clone())
                        .expect("no existing configuration");
                    true
                });

                // Now transition to `CoordinatorOperation::Prepare`
                let prepares: BTreeMap<_, _> = new_shares
                    .into_iter()
                    .map(|(id, share)| {
                        (id, (self.configuration.clone(), share))
                    })
                    .collect();
                self.op = CoordinatorOperation::Prepare {
                    prepares,
                    // Always include ourself
                    prepare_acks: BTreeSet::from([ctx.platform_id().clone()]),
                };

                info!(log, "Starting to prepare after collecting shares");
                self.send_msgs(ctx);
            }
            op => {
                warn!(
                    self.log,
                    "Share received when coordinator is not expecting it";
                    "op" => op.name(),
                    "epoch" => %epoch,
                    "from" => %from
                );
            }
        }
    }
}

/// What should the coordinator be doing?
pub enum CoordinatorOperation {
    CollectShares {
        old_epoch: Epoch,
        old_collected_shares: BTreeMap<PlatformId, Share>,

        // These are new shares that the coordinator created that we carry along
        // until we get to `CoordinatorOperation::Prepare`
        new_shares: BTreeMap<PlatformId, Share>,
    },
    // We haven't started implementing this yet
    // Epoch is always 0
    CollectLrtqShares {
        members: BTreeMap<PlatformId, ShareDigestLrtq>,
        shares: BTreeMap<PlatformId, LrtqShare>,
    },
    Prepare {
        /// The set of Prepares to send to each node
        prepares: BTreeMap<PlatformId, (Configuration, Share)>,

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

    /// Return the members that have acked prepares, if the current operation
    /// is `Prepare`. Otherwise return an empty set.
    pub fn acked_prepares(&self) -> BTreeSet<PlatformId> {
        if let CoordinatorOperation::Prepare { prepare_acks, .. } = self {
            prepare_acks.clone()
        } else {
            BTreeSet::new()
        }
    }
}
