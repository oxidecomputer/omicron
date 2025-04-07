// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A trust quorum node that implements the trust quorum protocol

use crate::validators::{ReconfigurationError, ValidatedReconfigureMsg};
use crate::{
    CoordinatorState, Envelope, PersistentState, PlatformId, messages::*,
};

use slog::{Logger, o};
use std::time::Instant;

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
    ) -> Result<Option<PersistentState>, ReconfigurationError> {
        let Some(validated_msg) = ValidatedReconfigureMsg::new(
            &self.log,
            &self.platform_id,
            msg,
            (&self.persistent_state).into(),
            self.coordinator_state.as_ref().map(|cs| &cs.reconfigure_msg),
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

    // Send any required messages as a reconfiguration coordinator
    fn send_coordinator_msgs(
        &mut self,
        now: Instant,
        outbox: &mut Vec<Envelope>,
    ) {
        // This function is going to be called unconditionally in `tick`
        // callbacks. In this case we may not actually be a coordinator. We just
        // ignore the call in that case.
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
            let (coordinator_state, my_prepare_msg) =
                CoordinatorState::new_uninitialized(
                    self.platform_id.clone(),
                    now,
                    msg,
                )?;
            self.coordinator_state = Some(coordinator_state);
            // Add the prepare to our `PersistentState`
            self.persistent_state
                .prepares
                .insert(my_prepare_msg.config.epoch, my_prepare_msg);

            return Ok(Some(self.persistent_state.clone()));
        }

        // We have a committed configuration that is not LRTQ
        let config =
            self.persistent_state.last_committed_configuration().unwrap();

        self.coordinator_state = Some(CoordinatorState::new_reconfiguration(
            self.platform_id.clone(),
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
        assert_eq!(persistent_state.generation, 0);
        assert!(persistent_state.lrtq.is_none());
        assert!(persistent_state.commits.is_empty());
        assert!(persistent_state.decommissioned.is_none());
        // The only `PrepareMsg` is this one for the first epoch
        assert_eq!(persistent_state.prepares.len(), 1);

        // Extract the configuration for our initial prepare msg
        let config = &persistent_state
            .prepares
            .get(&input.reconfigure_msg.epoch)
            .unwrap()
            .config;

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
            envelope.msg,
            PeerMsg::Prepare(PrepareMsg { config: prepare_config, .. }) => {
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
