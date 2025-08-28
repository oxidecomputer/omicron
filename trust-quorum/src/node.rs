// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A trust quorum node that implements the trust quorum protocol
//!
//! Nodes respond to function calls synchronously. They do not queue up replies
//! for later. If a request like a `prepare_and_commit` operation arrives from
//! Nexus, a node may not be ready to commit immediately. It may have to reach
//! out to other nodes to get its configuration and collect shares. None of
//! this happens in one tick through this FSM. Instead, outgoing messages will
//! be queued for sending and the call will return. Higher level software must
//! keep track of incoming requests and poll this FSM for responses. This makes
//! the logical code here simpler at the cost of implementing tracking at higher
//! levels. Fortunately, tracking is easier with async code, which drives this
//! Node, and so this should not be problematic.

use crate::compute_key_share::KeyShareComputer;
use crate::validators::{
    MismatchedRackIdError, ReconfigurationError, ValidatedReconfigureMsg,
};
use crate::{
    Alarm, Configuration, CoordinatorState, Epoch, ExpungedMetadata,
    NodeHandlerCtx, PlatformId, messages::*,
};
use daft::{Diffable, Leaf};
use gfss::shamir::Share;
use omicron_uuid_kinds::RackUuid;
use slog::{Logger, error, info, o, warn};

/// An entity capable of participating in trust quorum
///
/// This is a `sans-io` implementation that is deterministic (except for
/// `RackSecretGeneration`, which currently hardcodes use of an OsRng). This
/// style is primarily for testing purposes.
#[derive(Debug, Clone, Diffable)]
pub struct Node {
    #[daft(ignore)]
    log: Logger,

    /// In memory state for when this node is coordinating a reconfiguration
    coordinator_state: Option<CoordinatorState>,

    /// In memory state for when this node is trying to compute its own key
    /// share for a committed epoch.
    key_share_computer: Option<KeyShareComputer>,
}

// For diffs we want to allow access to all fields, but not make them public in
// the `Node` type itself.
impl NodeDiff<'_> {
    pub fn coordinator_state(&self) -> Leaf<Option<&CoordinatorState>> {
        self.coordinator_state
    }

    pub fn key_share_computer(&self) -> Leaf<Option<&KeyShareComputer>> {
        self.key_share_computer
    }
}

#[cfg(feature = "danger_partial_eq_ct_wrapper")]
impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.coordinator_state == other.coordinator_state
            && self.key_share_computer == other.key_share_computer
    }
}

#[cfg(feature = "danger_partial_eq_ct_wrapper")]
impl Eq for Node {}

impl Node {
    pub fn new(log: &Logger, ctx: &mut impl NodeHandlerCtx) -> Node {
        let id_str = format!("{:?}", ctx.platform_id());
        let log =
            log.new(o!("component" => "trust-quorum", "platform_id" => id_str));
        Node { log, coordinator_state: None, key_share_computer: None }
    }

    /// Start coordinating a reconfiguration
    ///
    /// On success, queues messages that need sending to other nodes via `ctx`
    /// and returns a `PersistentState` which the caller must write to disk.
    ///
    /// For upgrading from LRTQ, use `coordinate_upgrade_from_lrtq`
    pub fn coordinate_reconfiguration(
        &mut self,
        ctx: &mut impl NodeHandlerCtx,
        msg: ReconfigureMsg,
    ) -> Result<(), ReconfigurationError> {
        let Some(validated_msg) = ValidatedReconfigureMsg::new(
            &self.log,
            ctx.platform_id(),
            msg,
            ctx.persistent_state().into(),
            self.coordinator_state.as_ref().map(|cs| cs.reconfigure_msg()),
        )?
        else {
            // This was an idempotent (duplicate) request.
            return Ok(());
        };

        if let Some(kcs) = &self.key_share_computer {
            // We know from our `ValidatedReconfigureMsg` that we haven't seen
            // a newer configuration and we have the correct last committed
            // configuration. Therefore if we are computing a key share, we must
            // be doing it for a stale commit and should cancel it.
            //
            // I don't think it's actually possible to hit this condition, but
            // we check anyway.
            info!(
                self.log,
                "Reconfiguration started. Cancelling key share compute";
                "reconfiguration_epoch" => %validated_msg.epoch(),
                "key_share_compute_epoch" => %kcs.config().epoch
            );
            self.key_share_computer = None;
        }

        self.set_coordinator_state(ctx, validated_msg)?;
        self.send_coordinator_msgs(ctx);
        Ok(())
    }

    pub fn is_computing_key_share(&self) -> bool {
        self.key_share_computer.is_some()
    }

    /// Commit a configuration
    ///
    /// This is triggered by a message from Nexus for each node in the
    /// configuration. Nexus sends that message once it records that enough
    /// `PrepareAck` messgages have been received by the coordinator.
    pub fn commit_configuration(
        &mut self,
        ctx: &mut impl NodeHandlerCtx,
        rack_id: RackUuid,
        epoch: Epoch,
    ) -> Result<(), CommitError> {
        {
            let ps = ctx.persistent_state();

            if let Some(expunged) = &ps.expunged {
                error!(
                    self.log,
                    "Commit attempted on expunged node";
                    "expunged_epoch" => %expunged.epoch,
                    "expunging_node" => %expunged.from
                );
                return Err(CommitError::Expunged {
                    epoch: expunged.epoch,
                    from: expunged.from.clone(),
                });
            }

            // If we have a configuration the rack id must match the one from
            // Nexus
            if let Some(ps_rack_id) = ps.rack_id() {
                if rack_id != ps_rack_id {
                    error!(
                        self.log,
                        "Commit attempted with invalid rack_id";
                        "expected" => %ps_rack_id,
                        "got" => %rack_id
                    );
                    return Err(CommitError::InvalidRackId(
                        MismatchedRackIdError {
                            expected: ps_rack_id,
                            got: rack_id,
                        },
                    ));
                }
            }

            // Is this an idempotent or stale request?
            if let Some(config) = ps.latest_committed_configuration() {
                if config.epoch >= epoch {
                    info!(
                        self.log,
                        "Received stale or idempotent commit from Nexus";
                        "latest_committed_epoch" => %config.epoch,
                        "received_epoch" => %epoch
                    );
                    return Ok(());
                }
            }

            // Do we have a config and share for this configuration?
            //
            // Nexus should only attempt to commit nodes that have acknowledged
            // a `Prepare`. The most likely reason that this has occurred
            // is that the node has lost its state on the M.2 drives. It can
            // recover by retrieving a config and recomputing its share, and
            // then committing. This will happen when it sees a request from
            // another node in a later configuration for a key share.
            //
            // We also issue an error response up to Nexus and it can then send
            // a `PrepareAndCommit` with all the nodes that have prepared up
            // to this point so that this node can retrieve a configuration and
            // compute its share for this configuration. This also allows nexus
            // to track the "error".
            if !ps.has_prepared(epoch) {
                error!(
                    self.log,
                    "tried to commit a configuration, but not prepared";
                    "epoch" => %epoch
                );
                return Err(CommitError::NotPrepared(epoch));
            }
        }

        ctx.update_persistent_state(|ps| ps.commits.insert(epoch));
        info!(self.log, "Committed configuration"; "epoch" => %epoch);

        // Are we currently coordinating for this epoch?
        // Stop coordinating if we are.
        if let Some(cs) = &self.coordinator_state {
            if cs.reconfigure_msg().epoch() == epoch {
                info!(
                    self.log,
                    "Stopping coordination due to commit";
                    "epoch" => %epoch
                );
                self.coordinator_state = None;
            }
        }

        Ok(())
    }

    /// A peer node has connected to this one
    pub fn on_connect(
        &mut self,
        ctx: &mut impl NodeHandlerCtx,
        peer: PlatformId,
    ) {
        ctx.add_connection(peer.clone());
        self.send_coordinator_msgs_to(ctx, peer.clone());
        if let Some(ksc) = &mut self.key_share_computer {
            ksc.on_connect(ctx, peer);
        }
    }

    /// A peer node has disconnected from this one
    pub fn on_disconnect(
        &mut self,
        ctx: &mut impl NodeHandlerCtx,
        peer: PlatformId,
    ) {
        ctx.remove_connection(&peer);
    }

    /// Handle a message from another node
    pub fn handle(
        &mut self,
        ctx: &mut impl NodeHandlerCtx,
        from: PlatformId,
        msg: PeerMsg,
    ) {
        if ctx.persistent_state().is_expunged() {
            warn!(
                self.log,
                "Received message while expunged. Dropping.";
                "from" => %from,
                "msg" => msg.kind.name()
            );
            return;
        }

        if let Some(rack_id) = ctx.persistent_state().rack_id() {
            if rack_id != msg.rack_id {
                error!(
                   self.log,
                   "Mismatched rack id";
                   "from" => %from,
                   "msg" => msg.kind.name(),
                   "expected" => %rack_id,
                   "got" => %msg.rack_id
                );
                return;
            }
        }
        match msg.kind {
            PeerMsgKind::PrepareAck(epoch) => {
                self.handle_prepare_ack(from, epoch);
            }
            PeerMsgKind::Prepare { config, share } => {
                self.handle_prepare(ctx, from, config, share);
            }
            PeerMsgKind::GetShare(epoch) => {
                self.handle_get_share(ctx, from, epoch);
            }
            PeerMsgKind::Share { epoch, share } => {
                self.handle_share(ctx, from, epoch, share);
            }
            PeerMsgKind::CommitAdvance(config) => {
                self.handle_commit_advance(ctx, from, config)
            }
            PeerMsgKind::Expunged(epoch) => {
                self.handle_expunged(ctx, from, epoch);
            }
            _ => todo!(
                "cannot handle message variant yet - not implemented: {msg:?}"
            ),
        }
    }

    /// Return the current state of the coordinator
    pub fn get_coordinator_state(&self) -> Option<&CoordinatorState> {
        self.coordinator_state.as_ref()
    }

    fn handle_prepare_ack(&mut self, from: PlatformId, epoch: Epoch) {
        // Are we coordinating for this epoch?
        if let Some(cs) = &mut self.coordinator_state {
            let current_epoch = cs.reconfigure_msg().epoch();
            if current_epoch == epoch {
                info!(self.log, "Received prepare ack";
                     "from" => %from,
                     "epoch" => %current_epoch,
                );
                // Store the ack in the coordinator state
                cs.ack_prepare(from);
            } else {
                // Log and drop message
                warn!(self.log, "Received prepare ack for wrong epoch";
                     "from" => %from,
                     "current_epoch" => %current_epoch,
                     "acked_epoch" => %epoch
                );
            }
        } else {
            warn!(self.log,
                  "Received prepare ack when not coordinating";
                   "from" => %from,
                   "acked_epoch" => %epoch
            );
        }
    }

    fn handle_expunged(
        &mut self,
        ctx: &mut impl NodeHandlerCtx,
        from: PlatformId,
        epoch: Epoch,
    ) {
        if let Some(config) = ctx.persistent_state().latest_config() {
            if epoch < config.epoch {
                // It's possible, but unlikely, that we were expunged at `epoch`
                // and later re-added to the trust-quorum, but the reply to
                // an old message is still floating in the network. This is
                // especially unlikely since, we should really have restarted
                // sprockets connections in this case. In any event, the race
                // condition exists at the protocol level, and so we handle it.
                if config.members.contains_key(ctx.platform_id()) {
                    let m = concat!(
                        "Received Expunged message for old epoch. ",
                        "We must have been re-added as a trust-quorum member."
                    );
                    warn!(
                        self.log,
                        "{m}";
                        "from" => %from,
                        "received_epoch" => %epoch,
                        "epoch" => %config.epoch
                    );
                }
                return;
            } else if epoch > config.epoch {
                let m = concat!(
                    "Received Expunged message for newer epoch. ",
                    "Recording expungement in persistent state."
                );
                warn!(
                    self.log,
                    "{m}";
                    "from" => %from,
                    "received_epoch" => %epoch,
                    "epoch" => %config.epoch
                );
                // Intentionally fall through
            } else {
                let m = concat!(
                    "Received Expunged message for latest known epoch. ",
                    "Recording expungement in persistent state."
                );
                warn!(
                    self.log,
                    "{m}";
                    "from" => %from,
                    "received_epoch" => %epoch,
                    "epoch" => %config.epoch
                );
                // Intentionally fall through
            }

            // Perform the actual expunge
            ctx.update_persistent_state(|ps| {
                ps.expunged = Some(ExpungedMetadata { epoch, from });
                true
            });

            // Stop coordinating and computing a key share
            self.coordinator_state = None;
            self.key_share_computer = None;
        } else {
            let m = concat!(
                "Received Expunge message, but we have no configurations. ",
                "We must have been factory reset already."
            );
            error!(
                self.log,
                "{m}";
                "from" => %from,
                "received_epoch" => %epoch
            );
        }
    }

    fn handle_commit_advance(
        &mut self,
        ctx: &mut impl NodeHandlerCtx,
        from: PlatformId,
        config: Configuration,
    ) {
        // The sender sent us a configuration even though we are not part of the
        // configuration. This is a bug on the sender's part, but doesn't rise
        // to the level of an alarm. Log an error.
        if !config.members.contains_key(ctx.platform_id()) {
            error!(
                self.log,
                "Received CommitAdvance, but not a member of configuration";
                "from" => %from,
                "epoch" => %config.epoch
            );
            return;
        }

        // We may have already advanced by the time we receive this message.
        // Let's check.
        if ctx.persistent_state().commits.contains(&config.epoch) {
            info!(
                self.log,
                "Received CommitAdvance, but already committed";
                "from" => %from,
                "epoch" => %config.epoch
            );
            return;
        }
        if ctx.persistent_state().has_prepared(config.epoch) {
            // Go ahead and commit
            info!(
                self.log,
                "Received CommitAdvance. Already prepared, now committing";
                "from" => %from,
                "epoch" => %config.epoch
            );
            ctx.update_persistent_state(|ps| ps.commits.insert(config.epoch));
            return;
        }

        // Do we have the configuration in our persistent state? If not save it.
        if let Some(existing) =
            ctx.persistent_state().configuration(config.epoch)
        {
            if existing != &config {
                error!(
                    self.log,
                    "Received a configuration mismatch";
                    "from" => %from,
                    "existing_config" => #?existing,
                    "received_config" => #?config
                );
                ctx.raise_alarm(Alarm::MismatchedConfigurations {
                    config1: (*existing).clone(),
                    config2: config.clone(),
                    from: from.clone(),
                });
                return;
            }
        } else {
            ctx.update_persistent_state(|ps| {
                ps.configs.insert_unique(config.clone()).expect("new config");
                true
            });
        }

        // Are we coordinating for an older epoch? If so, cancel.
        if let Some(cs) = &self.coordinator_state {
            let coordinating_epoch = cs.reconfigure_msg().epoch();
            if coordinating_epoch < config.epoch {
                info!(
                    self.log,
                    "Received CommitAdvance. Cancelling stale coordination";
                    "from" => %from,
                    "coordinating_epoch" => %coordinating_epoch,
                    "received_epoch" => %config.epoch
                );
                self.coordinator_state = None;
                // Intentionally fall through
            } else if coordinating_epoch == config.epoch {
                info!(
                    self.log,
                    "Received CommitAdvance while coordinating for same epoch!";
                    "from" => %from,
                    "epoch" => %config.epoch
                );
                return;
            } else {
                info!(
                    self.log,
                    "Received CommitAdvance for stale epoch while coordinating";
                    "from" => %from,
                    "received_epoch" => %config.epoch,
                    "coordinating_epoch" => %coordinating_epoch
                );
                return;
            }
        }

        // Are we already trying to compute our share for this config?
        if let Some(ksc) = &mut self.key_share_computer {
            if ksc.config().epoch > config.epoch {
                let msg = concat!(
                    "Received stale CommitAdvance. ",
                    "Already computing for later epoch"
                );
                info!(
                    self.log,
                    "{msg}";
                    "from" => %from,
                    "epoch" => %ksc.config().epoch,
                    "received_epoch" => %config.epoch
                );
                return;
            } else if ksc.config().epoch == config.epoch {
                info!(
                    self.log,
                    "Received CommitAdvance while already computing share";
                    "from" => %from,
                    "epoch" => %config.epoch
                );
                return;
            } else {
                info!(
                    self.log,
                    "Received CommitAdvance while computing share for old epoch";
                    "from" => %from,
                    "epoch" => %ksc.config().epoch,
                    "received_epoch" => %config.epoch
                );
                // Intentionally fall through
            }
        }

        // We either were collecting shares for an old epoch or haven't started
        // yet.
        self.key_share_computer =
            Some(KeyShareComputer::new(&self.log, ctx, config));
    }

    fn handle_get_share(
        &mut self,
        ctx: &mut impl NodeHandlerCtx,
        from: PlatformId,
        epoch: Epoch,
    ) {
        if let Some(latest_committed_config) =
            ctx.persistent_state().latest_committed_configuration()
        {
            if latest_committed_config.epoch > epoch {
                if !latest_committed_config.members.contains_key(&from) {
                    info!(
                        self.log,
                        "Received a GetShare message from expunged node";
                        "from" => %from,
                        "latest_committed_epoch" =>
                            %latest_committed_config.epoch,
                        "requested_epoch" => %epoch
                    );
                    ctx.send(
                        from,
                        PeerMsgKind::Expunged(latest_committed_config.epoch),
                    );
                    return;
                }
                info!(
                    self.log,
                    concat!(
                        "Received 'GetShare' from stale node. ",
                        "Responded with 'CommitAdvance'."
                    );
                    "from" => %from,
                    "latest_committed_epoch" => %latest_committed_config.epoch,
                    "requested_epoch" => %epoch
                );
                ctx.send(
                    from,
                    PeerMsgKind::CommitAdvance(latest_committed_config.clone()),
                );
                return;
            }
        }

        // Do we have the configuration? Is the requesting peer a member?
        if let Some(config) = ctx.persistent_state().configuration(epoch) {
            if !config.members.contains_key(&from) {
                info!(
                    self.log,
                    "Received a GetShare message from expunged node";
                    "from" => %from,
                    "epoch" => %epoch
                );
                // Technically, this node does not yet know that the
                // configuration at `epoch` has been committed. However,
                // requesting nodes only ask for key shares when they know that
                // the configuration has been committed. Therefore, rather than
                // introduce a new message such as `NotAMember`, we inform the
                // requesting node that they have been expunged.
                ctx.send(from, PeerMsgKind::Expunged(epoch));
                return;
            }
        }

        // If we have the share for the requested epoch, we always return it. We
        // know that it is at least as new as the last committed epoch. We might
        // not have learned about the configuration being committed yet, but
        // other nodes have and may need the share to unlock when the control
        // plane is not up yet.
        //
        // See RFD 238 section 5.3.3
        //
        if let Some(share) = ctx.persistent_state().shares.get(&epoch) {
            info!(
                self.log,
                "Received 'GetShare'. Responded with 'Share'.";
                "from" => %from,
                "epoch" => %epoch
            );
            ctx.send(from, PeerMsgKind::Share { epoch, share: share.clone() });
        } else {
            // TODO: We may want to return a `NoSuchShare(epoch)` reply if we don't
            // have the share, but it's not strictly necessary. It would only be for
            // logging/debugging purposes at the requester.
            info!(
                self.log,
                "Received 'GetShare', but it's missing.";
                "from" => %from,
                "epoch" => %epoch
            );
        }
    }

    fn handle_share(
        &mut self,
        ctx: &mut impl NodeHandlerCtx,
        from: PlatformId,
        epoch: Epoch,
        share: Share,
    ) {
        if let Some(cs) = &mut self.coordinator_state {
            cs.handle_share(ctx, from, epoch, share);
        } else if let Some(ksc) = &mut self.key_share_computer {
            if ksc.handle_share(ctx, from, epoch, share) {
                // We're have completed computing our share and saved it to
                // our persistent state. We have also marked the configuration
                // committed.
                self.key_share_computer = None;
            }
        } else {
            warn!(
                self.log,
                "Received share when not coordinating or computing share";
                "from" => %from,
                "epoch" => %epoch
            );
        }
    }

    fn handle_prepare(
        &mut self,
        ctx: &mut impl NodeHandlerCtx,
        from: PlatformId,
        config: Configuration,
        share: Share,
    ) {
        // We don't ack stale prepares as the system has moved on. Eventually
        // the coordinator that sent this message will learn about the new
        // committed configuration. We could safely save the information,
        // but it doesn't help us, and we don't want to ack to an out of date
        // coordinator.
        let latest_committed_epoch =
            ctx.persistent_state().latest_committed_epoch().unwrap_or(Epoch(0));
        if latest_committed_epoch > config.epoch {
            warn!(self.log, "Received stale prepare";
                  "from" => %from,
                  "prepare_epoch" => %config.epoch,
                  "latest_committed_epoch" => %latest_committed_epoch
            );
            return;
        }

        if !config.members.contains_key(ctx.platform_id()) {
            error!(
                self.log,
                "Received Prepare when not a member of configuration";
                "from" => %from,
                "prepare_epoch" => %config.epoch
            );
            return;
        }

        // We always save the config and share if we haven't committed a later
        // configuration. If we have seen a newer `Prepare`, it's possible
        // that that configuration will not commit, and the latest committed
        // configuration is the one for the prepare just received. In that case
        // this message was delayed and we missed the commit. We save the information
        // in case we need it later. There's no harm in this.
        //
        // Note: We don't actually do this in the TLA+ because we want to check
        // there if we can receive a `PrepareAndCommit` message and move the state
        // along that way.
        let msg_epoch = config.epoch;
        let mutated = ctx.update_persistent_state(|ps| {
            // Persistent state was updated if either a share or config
            // was inserted.
            let share_inserted =
                ps.shares.insert(config.epoch, share).is_none();
            let config_inserted = ps.configs.insert_unique(config).is_ok();
            share_inserted || config_inserted
        });
        if mutated {
            info!(self.log, "Prepared configuration";
                  "from" => %from,
                  "epoch" => %msg_epoch
            );
        }
        // If we are coordinating for an older epoch, then we should stop
        // coordinating. The configuration at this epoch will either never
        // commit, or has already committed without us learning about it from
        // Nexus. In either case the rest of the system has moved on and we
        // should stop coordinating.
        if let Some(cs) = &self.coordinator_state {
            if msg_epoch > cs.reconfigure_msg().epoch() {
                // This prepare is for a newer configuration than the one we are
                // currently coordinating. We must cancel our coordination as Nexus
                // has moved on.
                let cancel_msg = concat!(
                    "Received a prepare for newer configuration.",
                    "Cancelling our coordination."
                );
                info!(self.log, "{cancel_msg}";
                    "msg_epoch" => %msg_epoch,
                    "epoch" => %cs.reconfigure_msg().epoch(),
                    "from" => %from
                );
                self.coordinator_state = None;
            }
        }

        // Ack regardless of whether this is a new or idempotent request
        ctx.send(from, PeerMsgKind::PrepareAck(msg_epoch));
    }

    // Send any required messages as a reconfiguration coordinator
    fn send_coordinator_msgs(&mut self, ctx: &mut impl NodeHandlerCtx) {
        // This function is called unconditionally in `tick` callbacks. In this
        // case we may not actually be a coordinator. We ignore the call in
        // that case.
        if let Some(c) = self.coordinator_state.as_mut() {
            c.send_msgs(ctx);
        }
    }

    // Send any required messages as a reconfiguration coordinator
    fn send_coordinator_msgs_to(
        &mut self,
        ctx: &mut impl NodeHandlerCtx,
        platform_id: PlatformId,
    ) {
        // This function is called unconditionally in `tick` callbacks. In this
        // case we may not actually be a coordinator. We ignore the call in
        // that case.
        if let Some(c) = self.coordinator_state.as_mut() {
            c.send_msgs_to(ctx, platform_id);
        }
    }

    /// Start coordinating a reconfiguration
    ///
    /// By the time we get here, we know that we are not upgrading from LRTQ as
    /// we have a `ValidatedReconfigureMsg`.
    fn set_coordinator_state(
        &mut self,
        ctx: &mut impl NodeHandlerCtx,
        msg: ValidatedReconfigureMsg,
    ) -> Result<(), ReconfigurationError> {
        let log = self.log.new(o!("component" => "tq-coordinator-state"));

        // We have no committed configuration or lrtq ledger
        if ctx.persistent_state().is_uninitialized() {
            let (coordinator_state, my_config, my_share) =
                CoordinatorState::new_uninitialized(log, msg)?;
            self.coordinator_state = Some(coordinator_state);
            ctx.update_persistent_state(move |ps| {
                ps.shares.insert(my_config.epoch, my_share);
                ps.configs.insert_unique(my_config).expect("empty state");
                true
            });

            return Ok(());
        }

        // We have a committed configuration that is not LRTQ
        let (config, our_share) = ctx
            .persistent_state()
            .latest_committed_config_and_share()
            .expect("committed configuration exists");

        self.coordinator_state = Some(CoordinatorState::new_reconfiguration(
            log,
            msg,
            config,
            our_share.clone(),
        )?);

        Ok(())
    }
}

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum CommitError {
    #[error("invalid rack id")]
    InvalidRackId(
        #[from]
        #[source]
        MismatchedRackIdError,
    ),
    #[error("cannot commit: not prepared for epoch {0}")]
    NotPrepared(Epoch),
    #[error("cannot commit: expunged at epoch {epoch} by {from}")]
    Expunged { epoch: Epoch, from: PlatformId },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Epoch, NodeCallerCtx, NodeCommonCtx, NodeCtx, Threshold};
    use assert_matches::assert_matches;
    use omicron_test_utils::dev::test_setup_log;
    use omicron_uuid_kinds::RackUuid;
    use proptest::prelude::*;
    use test_strategy::{Arbitrary, proptest};

    fn arb_member() -> impl Strategy<Value = PlatformId> {
        (0..255u8).prop_map(|serial| {
            PlatformId::new("test".into(), serial.to_string())
        })
    }

    // Generate a `ReconfigureMsg` for an intial configuration
    //
    // This configuration is always valid for a node with empty
    // `PersistentState`
    fn arb_initial_reconfigure_msg() -> impl Strategy<Value = ReconfigureMsg> {
        proptest::collection::btree_set(arb_member(), 3..10).prop_map(
            |members| ReconfigureMsg {
                rack_id: RackUuid::new_v4(),
                epoch: Epoch(1),
                last_committed_epoch: None,
                members: members.clone(),
                threshold: Threshold(members.len() as u8 - 1),
            },
        )
    }

    #[derive(Arbitrary, Debug)]
    pub struct InitialReconfigurationInput {
        #[strategy(arb_initial_reconfigure_msg())]
        reconfigure_msg: ReconfigureMsg,
    }

    /// Start coordinating a reconfiguration for a new trust quorum
    #[proptest]
    fn initial_configuration(input: InitialReconfigurationInput) {
        let logctx = test_setup_log("initial_configuration");
        let my_platform_id =
            input.reconfigure_msg.members.first().unwrap().clone();
        let mut ctx = NodeCtx::new(my_platform_id.clone());
        let mut node = Node::new(&logctx.log, &mut ctx);

        // Simulate connections from all other nodes
        for peer in input.reconfigure_msg.members.iter().skip(1) {
            node.on_connect(&mut ctx, peer.clone());
        }

        node.coordinate_reconfiguration(
            &mut ctx,
            input.reconfigure_msg.clone(),
        )
        .expect("success");

        // An initial configuraration always causes a change to persistent state
        assert!(ctx.persistent_state_change_check_and_reset());
        // Checking if the persistent state has changed above cleared the bit
        assert!(!ctx.persistent_state_change_check_and_reset());

        let persistent_state = ctx.persistent_state().clone();

        // A PersistentState should always be returned
        // It should include the `PrepareMsg` for this node.
        assert!(persistent_state.lrtq.is_none());
        assert!(persistent_state.commits.is_empty());
        assert!(persistent_state.expunged.is_none());
        assert_eq!(persistent_state.configs.len(), 1);
        assert_eq!(persistent_state.shares.len(), 1);

        // Extract the configuration for our initial prepare msg
        let config =
            persistent_state.configs.get(&input.reconfigure_msg.epoch).unwrap();

        assert_eq!(config.epoch, input.reconfigure_msg.epoch);
        assert_eq!(config.coordinator, *ctx.platform_id());
        assert_eq!(config.members.len(), input.reconfigure_msg.members.len());
        assert_eq!(config.threshold, input.reconfigure_msg.threshold);
        assert!(config.encrypted_rack_secrets.is_none());

        // Ensure that prepare messages are properly put in the outbox to be
        // sent by the I/O parts of the codebase
        assert_eq!(ctx.num_envelopes(), config.members.len() - 1);
        for envelope in ctx.drain_envelopes() {
            assert_matches!(
            envelope.msg.kind,
            PeerMsgKind::Prepare{ config: prepare_config, .. } => {
                assert_eq!(*config, prepare_config);
            });
            assert_eq!(envelope.from, config.coordinator);

            // We don't send a prepare to ourselves. We put it in our
            // `PersistentState` directly.
            assert_ne!(envelope.to, config.coordinator);
        }

        logctx.cleanup_successful();
    }
}
