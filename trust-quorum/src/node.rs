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

use crate::validators::{ReconfigurationError, ValidatedReconfigureMsg};
use crate::{CoordinatorState, Epoch, NodeHandlerCtx, PlatformId, messages::*};
use slog::{Logger, error, o, warn};

/// An entity capable of participating in trust quorum
///
/// This is a `sans-io` implementation that is deterministic (except for
/// `RackSecretGeneration`, which currently hardcodes use of an OsRng). This
/// style is primarily for testing purposes.
pub struct Node {
    log: Logger,

    /// In memory state for when this node is coordinating a reconfiguration
    coordinator_state: Option<CoordinatorState>,
}

impl Node {
    pub fn new(log: Logger, ctx: &mut impl NodeHandlerCtx) -> Node {
        let id_str = format!("{:?}", ctx.platform_id());
        let log =
            log.new(o!("component" => "trust-quorum", "platform_id" => id_str));
        Node { log, coordinator_state: None }
    }

    /// Start coordinating a reconfiguration
    ///
    /// On success, puts messages that need sending to other nodes in `outbox`
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

        self.set_coordinator_state(ctx, validated_msg)?;
        self.send_coordinator_msgs(ctx);
        Ok(())
    }

    /// Process a timer tick
    ///
    /// Ticks are issued by the caller in order to move the protocol forward.
    /// The current time is passed in to make the calls deterministic.
    pub fn tick(&mut self, ctx: &mut impl NodeHandlerCtx) {
        self.send_coordinator_msgs(ctx);
    }

    /// Handle a message from another node
    pub fn handle(
        &mut self,
        ctx: &mut impl NodeHandlerCtx,
        from: PlatformId,
        msg: PeerMsg,
    ) {
        if let Some(rack_id) = ctx.persistent_state().rack_id() {
            if rack_id != msg.rack_id {
                error!(self.log, "Mismatched rack id";
                       "from" => %from,
                       "msg" => msg.kind.name(),
                       "expected" => %rack_id,
                       "got" => %msg.rack_id);
                return;
            }
        }
        match msg.kind {
            PeerMsgKind::PrepareAck(epoch) => {
                self.handle_prepare_ack(from, epoch);
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
            warn!(
                self.log,
                "Received prepare ack when not coordinating";
                "from" => %from,
                "acked_epoch" => %epoch
            );
        }
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

    /// Set the coordinator state and conditionally set and return the
    /// persistent state depending upon whether the node is currently
    /// coordinating and what its persistent state is.
    ///
    /// By the time we get here, we know that we are not upgrading from LRTQ as
    /// we have a `ValidatedReconfigureMsg`.
    fn set_coordinator_state(
        &mut self,
        ctx: &mut impl NodeHandlerCtx,
        msg: ValidatedReconfigureMsg,
    ) -> Result<(), ReconfigurationError> {
        // We have no committed configuration or lrtq ledger
        if ctx.persistent_state().is_uninitialized() {
            let (coordinator_state, my_config, my_share) =
                CoordinatorState::new_uninitialized(
                    self.log.clone(),
                    ctx.now(),
                    msg,
                )?;
            self.coordinator_state = Some(coordinator_state);
            ctx.update_persistent_state(move |ps| {
                ps.shares.insert(my_config.epoch, my_share);
                ps.configs.insert_unique(my_config).expect("empty state");
                true
            });

            return Ok(());
        }

        // We have a committed configuration that is not LRTQ
        let config =
            ctx.persistent_state().latest_committed_configuration().unwrap();

        self.coordinator_state = Some(CoordinatorState::new_reconfiguration(
            self.log.clone(),
            ctx.now(),
            msg,
            &config,
        )?);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Epoch, NodeCallerCtx, NodeCommonCtx, NodeCtx, Threshold};
    use assert_matches::assert_matches;
    use omicron_test_utils::dev::test_setup_log;
    use omicron_uuid_kinds::RackUuid;
    use proptest::prelude::*;
    use std::time::Duration;
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
                retry_timeout: Duration::from_millis(100),
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
        let mut node = Node::new(logctx.log.clone(), &mut ctx);

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
