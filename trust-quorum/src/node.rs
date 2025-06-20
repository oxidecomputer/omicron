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
use crate::{
    CoordinatorState, Envelope, Epoch, PersistentState, PlatformId, messages::*,
};
use slog::{Logger, error, o, warn};
use std::time::Instant;

/// An entity capable of participating in trust quorum
///
/// This is a `sans-io` implementation that is deterministic (except for
/// `RackSecretGeneration`, which currently hardcodes use of an OsRng). This
/// style is primarily for testing purposes.
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
    ) -> Result<Option<PersistentState>, ReconfigurationError> {
        let Some(validated_msg) = ValidatedReconfigureMsg::new(
            &self.log,
            &self.platform_id,
            msg,
            (&self.persistent_state).into(),
            self.coordinator_state.as_ref().map(|cs| cs.reconfigure_msg()),
        )?
        else {
            // This was an idempotent (duplicate) request.
            return Ok(None);
        };

        let persistent_state =
            self.set_coordinator_state(now, validated_msg)?;
        self.send_coordinator_msgs(now, outbox);
        Ok(persistent_state)
    }

    /// Process a timer tick
    ///
    /// Ticks are issued by the caller in order to move the protocol forward.
    /// The current time is passed in to make the calls deterministic.
    pub fn tick(&mut self, now: Instant, outbox: &mut Vec<Envelope>) {
        self.send_coordinator_msgs(now, outbox);
    }

    /// Handle a message from another node
    pub fn handle(
        &mut self,
        _now: Instant,
        _outbox: &mut Vec<Envelope>,
        from: PlatformId,
        msg: PeerMsg,
    ) -> Option<PersistentState> {
        if let Some(rack_id) = self.persistent_state.rack_id() {
            if rack_id != msg.rack_id {
                error!(self.log, "Mismatched rack id";
                       "from" => %from,
                       "msg" => msg.kind.name(),
                       "expected" => %rack_id,
                       "got" => %msg.rack_id);
                return None;
            }
        }
        match msg.kind {
            PeerMsgKind::PrepareAck(epoch) => {
                self.handle_prepare_ack(from, epoch);
                None
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
    fn send_coordinator_msgs(
        &mut self,
        now: Instant,
        outbox: &mut Vec<Envelope>,
    ) {
        // This function is called unconditionally in `tick` callbacks. In this
        // case we may not actually be a coordinator. We ignore the call in
        // that case.
        if let Some(c) = self.coordinator_state.as_mut() {
            c.send_msgs(now, outbox);
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
        now: Instant,
        msg: ValidatedReconfigureMsg,
    ) -> Result<Option<PersistentState>, ReconfigurationError> {
        // We have no committed configuration or lrtq ledger
        if self.persistent_state.is_uninitialized() {
            let (coordinator_state, my_config, my_share) =
                CoordinatorState::new_uninitialized(
                    self.log.clone(),
                    now,
                    msg,
                )?;
            self.coordinator_state = Some(coordinator_state);
            self.persistent_state.shares.insert(my_config.epoch, my_share);
            self.persistent_state
                .configs
                .insert_unique(my_config)
                .expect("empty state");

            return Ok(Some(self.persistent_state.clone()));
        }

        // We have a committed configuration that is not LRTQ
        let config =
            self.persistent_state.latest_committed_configuration().unwrap();

        self.coordinator_state = Some(CoordinatorState::new_reconfiguration(
            self.log.clone(),
            now,
            msg,
            &config,
        )?);

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::{Epoch, Threshold};

    use super::*;
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
        let mut node = Node::new(
            logctx.log.clone(),
            my_platform_id.clone(),
            PersistentState::empty(),
        );

        let mut outbox = Vec::new();
        let persistent_state = node
            .coordinate_reconfiguration(
                Instant::now(),
                &mut outbox,
                input.reconfigure_msg.clone(),
            )
            .expect("success")
            .expect("persistent state");

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
        assert_eq!(config.coordinator, *node.platform_id());
        assert_eq!(config.members.len(), input.reconfigure_msg.members.len());
        assert_eq!(config.threshold, input.reconfigure_msg.threshold);
        assert!(config.previous_configuration.is_none());

        // Ensure that prepare messages are properly put in the outbox to be
        // sent by the I/O parts of the codebase
        assert_eq!(outbox.len(), config.members.len() - 1);
        for envelope in outbox {
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
