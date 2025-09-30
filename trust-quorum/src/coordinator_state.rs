// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! State of a reconfiguration coordinator inside a [`crate::Node`]

use crate::NodeHandlerCtx;
use crate::configuration::{ConfigurationDiff, ConfigurationError};
use crate::crypto::{LrtqShare, PlaintextRackSecrets, ReconstructedRackSecret};
use crate::validators::{
    ReconfigurationError, ValidatedLrtqUpgradeMsg, ValidatedReconfigureMsg,
};
use crate::{Configuration, Epoch, PeerMsgKind, BaseboardId, RackSecret};
use bootstore::trust_quorum::RackSecret as LrtqRackSecret;
use daft::{Diffable, Leaf};
use gfss::shamir::Share;
use slog::{Logger, error, info, o, warn};
use std::collections::{BTreeMap, BTreeSet};
use std::mem;

// A coordinator can be upgrading from LRTQ or reconfiguring a TQ config.
#[derive(Clone, Debug, PartialEq, Eq, Diffable)]
pub enum CoordinatingMsg {
    Upgrade(ValidatedLrtqUpgradeMsg),
    Reconfig(ValidatedReconfigureMsg),
}

impl CoordinatingMsg {
    pub fn epoch(&self) -> Epoch {
        match self {
            Self::Upgrade(msg) => msg.epoch(),
            Self::Reconfig(msg) => msg.epoch(),
        }
    }
}

/// The state of a reconfiguration coordinator.
///
/// A coordinator can be any trust quorum node that is a member of both the old
/// and new group. The coordinator is chosen by Nexus for a given epoch when a
/// trust quorum reconfiguration is triggered. Reconfiguration is only performed
/// when the control plane is up, as we use Nexus to persist prepares and ensure
/// commitment happens, even if the system crashes while committing. If a
/// rack crash (such as a power outage) occurs before nexus is informed of the
/// prepare acks, nexus will skip the epoch and start a new reconfiguration. This
/// allows progress to always be made with a full linearization of epochs.
#[derive(Clone, Debug, Diffable)]
pub struct CoordinatorState {
    #[daft(ignore)]
    log: Logger,

    /// A copy of the message used to start this coordination
    msg: CoordinatingMsg,

    /// Configuration that will get persisted inside a `Prepare` message in a
    /// `Node`s `PersistentState`, once it is possible to create the Prepare.
    configuration: Configuration,

    /// What is the coordinator currently doing
    op: CoordinatorOperation,
}

// For diffs we want to allow access to all fields, but not make them public in
// the `CoordinatorState` type itself.
impl<'daft> CoordinatorStateDiff<'daft> {
    pub fn msg(&self) -> Leaf<&CoordinatingMsg> {
        self.msg
    }

    pub fn configuration(&self) -> &ConfigurationDiff<'daft> {
        &self.configuration
    }

    pub fn op(&self) -> Leaf<&CoordinatorOperation> {
        self.op
    }
}

#[cfg(feature = "danger_partial_eq_ct_wrapper")]
impl PartialEq for CoordinatorState {
    fn eq(&self, other: &Self) -> bool {
        self.msg == other.msg
            && self.configuration == other.configuration
            && self.op == other.op
    }
}

#[cfg(feature = "danger_partial_eq_ct_wrapper")]
impl Eq for CoordinatorState {}

impl CoordinatorState {
    /// Start coordinating a reconfiguration for a brand new trust quorum
    ///
    /// Return the newly constructed `CoordinatorState` along with this node's
    /// `PrepareMsg` so that it can be persisted.
    pub fn new_uninitialized(
        log: &Logger,
        msg: ValidatedReconfigureMsg,
    ) -> Result<(CoordinatorState, Configuration, Share), ReconfigurationError>
    {
        let log = log.new(o!("component" => "tq-coordinator-state"));
        // Create a configuration for this epoch
        let (config, shares) = Configuration::new((&msg).into())?;

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
        log: &Logger,
        msg: ValidatedReconfigureMsg,
        latest_committed_config: &Configuration,
        our_latest_committed_share: Share,
    ) -> Result<CoordinatorState, ReconfigurationError> {
        let log = log.new(o!("component" => "tq-coordinator-state"));
        let (config, new_shares) = Configuration::new((&msg).into())?;

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

    pub fn new_upgrade_from_lrtq(
        log: &Logger,
        ctx: &mut impl NodeHandlerCtx,
        msg: ValidatedLrtqUpgradeMsg,
    ) -> Result<CoordinatorState, ConfigurationError> {
        let log = log.new(o!("component" => "tq-coordinator-state"));
        let (configuration, new_shares) = Configuration::new((&msg).into())?;

        info!(
            log,
            "Starting coordination for LRTQ upgrade on existing node";
            "epoch" => %configuration.epoch
        );

        // We must collect the LRTQ shares so we can recompute the LRTQ rack
        // secret.
        let op = CoordinatorOperation::CollectLrtqShares {
            collected_lrtq_shares: BTreeMap::from([(
                msg.coordinator_id().clone(),
                LrtqShare::new(
                    ctx.persistent_state()
                        .lrtq
                        .as_ref()
                        .expect("lrtq config exists")
                        .share
                        .clone(),
                ),
            )]),
            new_shares,
        };

        Ok(CoordinatorState {
            log,
            msg: CoordinatingMsg::Upgrade(msg),
            configuration,
            op,
        })
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
            msg: CoordinatingMsg::Reconfig(reconfigure_msg),
            configuration,
            op,
        }
    }

    /// Return the validated msg that started this reconfiguration
    pub fn msg(&self) -> &CoordinatingMsg {
        &self.msg
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
                    .filter(|&id| {
                        !old_collected_shares.contains_key(id)
                            && ctx.connected().contains(id)
                    })
                    .cloned()
                    .collect();
                for to in destinations {
                    ctx.send(to, PeerMsgKind::GetShare(*old_epoch));
                }
            }
            CoordinatorOperation::CollectLrtqShares {
                collected_lrtq_shares,
                ..
            } => {
                let destinations: Vec<_> = self
                    .configuration
                    .members
                    .keys()
                    .filter(|&id| {
                        !collected_lrtq_shares.contains_key(id)
                            && ctx.connected().contains(id)
                    })
                    .cloned()
                    .collect();
                for to in destinations {
                    ctx.send(to, PeerMsgKind::GetLrtqShare);
                }
            }
            CoordinatorOperation::Prepare { prepares, .. } => {
                for (platform_id, (config, share)) in prepares.iter() {
                    if ctx.connected().contains(&platform_id) {
                        ctx.send(
                            platform_id.clone(),
                            PeerMsgKind::Prepare {
                                config: config.clone(),
                                share: share.clone(),
                            },
                        );
                    }
                }
            }
        }
    }

    // Send any required messages to a newly connected node
    pub fn send_msgs_to(
        &mut self,
        ctx: &mut impl NodeHandlerCtx,
        to: BaseboardId,
    ) {
        match &self.op {
            CoordinatorOperation::CollectShares {
                old_epoch,
                old_collected_shares,
                ..
            } => {
                if !old_collected_shares.contains_key(&to)
                    && ctx.connected().contains(&to)
                    && ctx
                        .persistent_state()
                        .configuration(*old_epoch)
                        .expect("config exists")
                        .members
                        .contains_key(&to)
                {
                    ctx.send(to, PeerMsgKind::GetShare(*old_epoch));
                }
            }
            CoordinatorOperation::CollectLrtqShares {
                collected_lrtq_shares,
                ..
            } => {
                if !collected_lrtq_shares.contains_key(&to)
                    && ctx.connected().contains(&to)
                    && self.configuration.members.contains_key(&to)
                {
                    ctx.send(to, PeerMsgKind::GetLrtqShare);
                }
            }
            CoordinatorOperation::Prepare { prepares, .. } => {
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
    pub fn ack_prepare(&mut self, from: BaseboardId) {
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
                info!(
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
        from: BaseboardId,
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

                if !crate::validate_share(
                    &self.log,
                    &old_config,
                    &from,
                    epoch,
                    &share,
                ) {
                    // Logging done inside `validate_share`
                    return;
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

                // Take `new_shares` out of `self.op` so we can include them in
                // `Prepare` messages;
                let new_shares = mem::take(new_shares);

                // Start Preparing
                self.transition_to_preparing(
                    ctx,
                    log,
                    new_shares,
                    plaintext_secrets,
                );
            }
            op => {
                info!(
                    self.log,
                    "Share received when coordinator is not expecting it";
                    "op" => op.name(),
                    "epoch" => %epoch,
                    "from" => %from
                );
            }
        }
    }

    pub fn handle_lrtq_share(
        &mut self,
        ctx: &mut impl NodeHandlerCtx,
        from: BaseboardId,
        share: LrtqShare,
    ) {
        match &mut self.op {
            CoordinatorOperation::CollectLrtqShares {
                collected_lrtq_shares,
                new_shares,
            } => {
                let log = self.log.new(o!(
                    "last_committed_epoch" => "lrtq",
                    "new_epoch" => self.configuration.epoch.to_string()
                ));

                // Do we already have a share from this node?
                if collected_lrtq_shares.contains_key(&from) {
                    return;
                }

                // SAFETY: If we are collecting LRTQ shares it means we have
                // `LrtqShareData` in our `PersistentState`.
                let lrtq_share_data = ctx
                    .persistent_state()
                    .lrtq
                    .as_ref()
                    .expect("lrtq share data exists");

                // We don't know which share digest corresponds to which node
                // and so we check to ensure that the digest of the share is one
                // of the valid digests we expect.
                let digest = share.digest().into();
                if !lrtq_share_data.share_digests.contains(&digest) {
                    error!(
                        log,
                        "Received LrtqShare with invalid digest";
                        "from" => %from
                    );
                    return;
                }

                // We double check to ensure that a different node didn't send
                // the same share. This should never happen, or LRTQ would be
                // busted on existing racks, but we sanity check here anyway.
                if let Some((matching_id, _)) =
                    collected_lrtq_shares.iter().find(|(_, s)| {
                        let computed: bootstore::Sha3_256Digest =
                            s.digest().into();
                        computed == digest
                    })
                {
                    error!(
                        log,
                        "Received share with digest matching another node";
                        "from" => %from,
                        "matching_node" => %matching_id
                    );
                    return;
                }

                // We have a new, unique, LRTQ share. Save it.
                collected_lrtq_shares.insert(from, share);

                // Do we have enough shares to recompute the LRTQ rack secret?
                if collected_lrtq_shares.len()
                    < lrtq_share_data.threshold as usize
                {
                    return;
                }

                // Reconstruct the LRTQ rack secret
                let shares: Vec<_> = collected_lrtq_shares
                    .values()
                    .map(|s| s.inner().clone())
                    .collect();
                let lrtq_rack_secret: ReconstructedRackSecret =
                    match LrtqRackSecret::combine_shares(&shares) {
                        Ok(secret) => {
                            info!(
                                log,
                                "Successfully reconstructed LRTQ rack secret"
                            );
                            secret.into()
                        }
                        Err(err) => {
                            error!(
                                self.log,
                                "Failed to reconstruct LRTQ rack secret";
                                "err" => err.to_string()
                            );
                            return;
                        }
                    };

                // There are no old encrytped rack secrets for LRTQ
                // LRTQ is always at Epoch 1
                let mut plaintext_secrets = PlaintextRackSecrets::new();
                plaintext_secrets.insert(Epoch(1), lrtq_rack_secret);

                // Take `new_shares` out of `self.op` so we can include them in
                // `Prepare` messages;
                let new_shares = mem::take(new_shares);

                // Start Preparing
                self.transition_to_preparing(
                    ctx,
                    log,
                    new_shares,
                    plaintext_secrets,
                );
            }
            op => {
                info!(
                    self.log,
                    "LrtqShare received when coordinator is not expecting it";
                    "op" => op.name(),
                    "from" => %from
                );
            }
        }
    }

    // Make the jump from collecting shares or lrtq shares to sending out
    // prepare messages and waiting for acks.
    fn transition_to_preparing(
        &mut self,
        ctx: &mut impl NodeHandlerCtx,
        log: Logger,
        mut new_shares: BTreeMap<BaseboardId, Share>,
        plaintext_secrets: PlaintextRackSecrets,
    ) {
        let new_epoch = self.configuration.epoch;

        // Reconstruct the new rack secret from the shares we created
        // at coordination start time.
        let shares: Vec<_> = new_shares.values().cloned().collect();
        let new_rack_secret = match RackSecret::reconstruct(&shares) {
            Ok(secret) => {
                info!(log, "Successfully reconstructed new rack secret");
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

        // Now encrypt the set of old rack secrets with the new rack
        // secret.
        let new_encrypted_rack_secrets = match plaintext_secrets.encrypt(
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
        //
        // A new configuration is always created with a `None` value
        // for `encrypted_rack_secrets`, as it gets filled in here.
        //
        // If we change that it's a programmer error that will be caught
        // immediately by our tests.
        assert!(self.configuration.encrypted_rack_secrets.is_none());
        self.configuration.encrypted_rack_secrets =
            Some(new_encrypted_rack_secrets);

        // Update our persistent state
        //
        // We remove ourself because we don't send a `Prepare` message
        // to ourself.
        //
        // SAFETY: our share already exists at this point and has been
        // validated as part of the `Configuration` construction.
        let share =
            new_shares.remove(ctx.platform_id()).expect("my share exists");
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
            .map(|(id, share)| (id, (self.configuration.clone(), share)))
            .collect();
        self.op = CoordinatorOperation::Prepare {
            prepares,
            // Always include ourself
            prepare_acks: BTreeSet::from([ctx.platform_id().clone()]),
        };

        info!(log, "Starting to prepare after collecting shares");
        self.send_msgs(ctx);
    }
}

/// What should the coordinator be doing?
#[derive(Clone, Debug, Diffable)]
#[cfg_attr(feature = "danger_partial_eq_ct_wrapper", derive(PartialEq, Eq))]
pub enum CoordinatorOperation {
    CollectShares {
        old_epoch: Epoch,
        old_collected_shares: BTreeMap<BaseboardId, Share>,

        // These are new shares that the coordinator created that we carry along
        // until we get to `CoordinatorOperation::Prepare`
        new_shares: BTreeMap<BaseboardId, Share>,
    },
    CollectLrtqShares {
        collected_lrtq_shares: BTreeMap<BaseboardId, LrtqShare>,

        // These are new shares that the coordinator created that we carry along
        // until we get to `CoordinatorOperation::Prepare`
        new_shares: BTreeMap<BaseboardId, Share>,
    },
    Prepare {
        /// The set of Prepares to send to each node
        prepares: BTreeMap<BaseboardId, (Configuration, Share)>,

        /// Acknowledgements that the prepare has been received
        prepare_acks: BTreeSet<BaseboardId>,
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
    pub fn acked_prepares(&self) -> BTreeSet<BaseboardId> {
        if let CoordinatorOperation::Prepare { prepare_acks, .. } = self {
            prepare_acks.clone()
        } else {
            BTreeSet::new()
        }
    }
}
