// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A trust quorum node that implements the trust quorum protocol

use crate::errors::ReconfigurationError;
use crate::validators::ValidatedReconfigureMsg;
use crate::{
    Alarm, CoordinatorState, Envelope, Epoch, PersistentState, PlatformId,
    messages::*,
};
use gfss::shamir::Share;
use omicron_uuid_kinds::RackUuid;

use slog::{Logger, error, info, o, warn};
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

    /// Commit the configuration for the given epoch
    pub fn commit_reconfiguration(
        &mut self,
        epoch: Epoch,
        rack_id: RackUuid,
    ) -> Result<Option<PersistentState>, Alarm> {
        let Some(latest_prepare) = self.persistent_state.latest_prepare()
        else {
            // This is an erroneous commit attempt from nexus. We don't have
            // any prepares yet, but for some reason nexus thinks we do.
            //
            // Nexus should instead tell this node to retrieve a `Prepare`
            // from another node that has already committed.
            //
            // This is a less serious error than other invariant violations
            // since it can be recovered from. However, it is still worthy of an
            // alarm, as the most likely case is a  disk/ ledger failure.
            let latest_seen_epoch = None;
            let alarm = Alarm::MissingPrepare { epoch, latest_seen_epoch };
            error!(self.log, "{alarm}");
            return Err(alarm);
        };

        if latest_prepare.config.epoch < epoch {
            // We haven't seen this prepare yet, but Nexus thinks we have.
            // This is essentially the same case as above.
            let latest_seen_epoch = Some(latest_prepare.config.epoch);
            let alarm = Alarm::MissingPrepare { epoch, latest_seen_epoch };
            error!(self.log, "{alarm}");
            return Err(alarm);
        }

        if latest_prepare.config.epoch > epoch {
            // Only commit if we have a `PrepareMsg` and it's the latest
            // `PrepareMsg`.
            //
            // This forces a global ordering of `PrepareMsg`s, because it's
            // only possible to re-derive a key share in a `PrepareMsg` for the
            // current configuration.
            //
            // If we get a commit for an earlier prepare message, then we
            // shouldn't have been able to accept any later prepare messages,
            // because this commit hadn't been seen by this node yet. Prepare
            // messages are only accepted if the last committed epoch in the
            // message matches what the node has seen.
            //
            // If we get here, then it means that there is a bug in this code
            // that allowed the later prepare to be accepted, and therefore we
            // must raise an alarm.
            let alarm = Alarm::OutOfOrderCommit {
                last_prepared_epoch: latest_prepare.config.epoch,
                commit_epoch: epoch,
            };
            error!(self.log, "{alarm}");
            return Err(alarm);
        }

        // The epoch of the latest prepare matches the commit.
        // Do the rack_ids match up?
        if latest_prepare.config.rack_id != rack_id {
            let alarm = Alarm::CommitWithInvalidRackId {
                expected: latest_prepare.config.rack_id,
                got: rack_id,
            };
            error!(self.log, "{alarm}");
            return Err(alarm);
        }

        if self.persistent_state.last_committed_epoch() == Some(epoch) {
            info!(
                self.log,
                "Idempotent configuration - already committed";
                "epoch" => %epoch
            );
            return Ok(None);
        }

        // Success!
        info!(self.log, "Committed configuration"; "epoch" => %epoch);

        // Are we currently coordinating for this epoch? Stop if so.
        if self.coordinator_state.is_some() {
            info!(
                self.log,
                "Stopping coordination due to commit";
                "epoch" => %epoch
            );
            self.coordinator_state = None;
        }

        self.persistent_state.commits.insert(epoch);
        Ok(Some(self.persistent_state.clone()))
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
        now: Instant,
        outbox: &mut Vec<Envelope>,
        from: PlatformId,
        msg: PeerMsg,
    ) -> Result<Option<PersistentState>, Alarm> {
        match msg {
            PeerMsg::Prepare(msg) => self.handle_prepare(outbox, from, msg),
            PeerMsg::PrepareAck(epoch) => {
                self.handle_prepare_ack(from, epoch);
                Ok(None)
            }
            PeerMsg::Share { epoch, share } => {
                Ok(self.handle_share(now, outbox, from, epoch, share))
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

    // Handle a `PrepareMsg` from a coordinator
    fn handle_prepare(
        &mut self,
        outbox: &mut Vec<Envelope>,
        from: PlatformId,
        msg: PrepareMsg,
    ) -> Result<Option<PersistentState>, Alarm> {
        if let Some(rack_id) = self.persistent_state.rack_id() {
            if rack_id != msg.config.rack_id {
                let alarm = Alarm::PrepareWithInvalidRackId {
                    from,
                    expected: rack_id,
                    got: msg.config.rack_id,
                };
                error!(self.log, "{alarm}");
                return Err(alarm);
            }
        }
        if let Some(latest_prepare) = self.persistent_state.latest_prepare() {
            // Is this an old request?
            if msg.config.epoch < latest_prepare.config.epoch {
                warn!(self.log, "Received stale prepare";
                    "from" => %from,
                    "msg_epoch" => %msg.config.epoch,
                    "last_prepared_epoch" => %latest_prepare.config.epoch
                );
                return Ok(None);
            }

            // Idempotent request
            if msg.config == latest_prepare.config {
                return Ok(None);
            }

            // Does the last committed epoch match what we have?
            let msg_last_committed_epoch =
                msg.config.previous_configuration.as_ref().map(|p| p.epoch);
            let last_committed_epoch =
                self.persistent_state.last_committed_epoch();
            if msg_last_committed_epoch != last_committed_epoch {
                // If the msg contains an older last_committed_epoch than what
                // we have, then out of order commits have occurred, as we know
                // this prepare is later than what we've seen. This is a critical
                // protocol invariant that has been violated.
                //
                // If the msg contains a newer last_committed_epoch than what
                // we have, then we have likely missed a commit and are behind
                // by more than one reconfiguration. The protocol currently does
                // not allow this. Future protocol implementations may provide a
                // capability to "jump" configurations.
                //
                // If there is a `None`/`Some` mismatch, then again, an invariant
                // has been violated somewhere. The coordinator should know
                // whether this is a "new" node or not, which would have a "None"
                // configuration.
                let alarm = Alarm::PrepareLastCommittedEpochMismatch {
                    from,
                    prepare_last_committed_epoch: msg_last_committed_epoch,
                    persisted_prepare_last_committed_epoch:
                        last_committed_epoch,
                };
                error!(self.log, "{alarm}");
                return Err(alarm);
            }

            // The prepare is up-to-date with respect to our persistent state
        };

        // Are we currently trying to coordinate?
        if let Some(cs) = &self.coordinator_state {
            let coordinating_epoch = cs.reconfigure_msg().epoch();
            if coordinating_epoch > msg.config.epoch {
                warn!(self.log, "Received stale prepare while coordinating";
                    "from" => %from,
                    "msg_epoch" => %msg.config.epoch,
                    "epoch" => %cs.reconfigure_msg().epoch()
                );
                return Ok(None);
            }
            if coordinating_epoch == msg.config.epoch {
                let alarm = Alarm::DifferentNodesCoordinatingSameEpoch {
                    epoch: coordinating_epoch,
                    them: from,
                    us: self.platform_id.clone(),
                };
                error!(self.log, "{alarm}");
                return Err(alarm);
            }

            // This prepare is for a newer configuration than the one we are
            // currently coordinating. We must cancel our coordination as Nexus
            // has moved on.
            let cancel_msg = concat!(
                "Received a prepare for newer configuration.",
                "Cancelling our coordination."
            );
            info!(self.log, "{cancel_msg}";
                "msg_epoch" => %msg.config.epoch,
                "epoch" => %coordinating_epoch,
                "from" => %from
            );
            self.coordinator_state = None;
        }

        // Acknowledge the `PrepareMsg`
        self.send(from, outbox, PeerMsg::PrepareAck(msg.config.epoch));

        // Add the prepare to our `PersistentState`
        self.persistent_state.prepares.insert(msg.config.epoch, msg);

        Ok(Some(self.persistent_state.clone()))
    }

    // Handle receipt of a `PrepareAck` message
    fn handle_prepare_ack(&mut self, from: PlatformId, epoch: Epoch) {
        // Are we coordinating for this epoch?
        if let Some(cs) = &mut self.coordinator_state {
            let current_epoch = cs.reconfigure_msg().epoch();
            if current_epoch == epoch {
                cs.record_prepare_ack(from);
            } else {
                warn!(self.log, "Received prepare ack for wrong epoch";
                    "from" => %from,
                    "current_epoch" => %current_epoch,
                    "acked_epoch" => %epoch
                );
                // Ack is intentionally dropped here
            }
        } else {
            warn!(
                self.log,
                "Received prepare ack when not coordinating";
                "from" => %from,
                "acked_epoch" => %epoch
            );
            // Ack is intentionally dropped here
        }
    }

    // Handle receipt of a `Share` message
    fn handle_share(
        &mut self,
        now: Instant,
        outbox: &mut Vec<Envelope>,
        from: PlatformId,
        epoch: Epoch,
        share: Share,
    ) -> Option<PersistentState> {
        // Are we coordinating and expecting shares for `epoch`, which must
        // be the last committed epoch?
        if let Some(cs) = &mut self.coordinator_state {
            if let Some(my_prepare_msg) =
                cs.handle_share(now, outbox, from, epoch, share)
            {
                // Add the prepare to our `PersistentState`
                self.persistent_state
                    .prepares
                    .insert(my_prepare_msg.config.epoch, my_prepare_msg);

                return Some(self.persistent_state.clone());
            }
        } else {
            warn!(
                self.log,
                "Received share when not coordinating";
                "from" => %from,
                "epoch" => %epoch
            );
        }
        None
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

    // Package a message in an envelope and put it in the outbox
    fn send(&self, to: PlatformId, outbox: &mut Vec<Envelope>, msg: PeerMsg) {
        outbox.push(Envelope { to, from: self.platform_id.clone(), msg });
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
                    self.log.clone(),
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

        // Safety: We have a committed configuration that is not LRTQ
        let our_share =
            self.persistent_state.last_committed_key_share().unwrap();
        let config =
            self.persistent_state.last_committed_configuration().unwrap();

        self.coordinator_state = Some(CoordinatorState::new_reconfiguration(
            self.log.clone(),
            now,
            msg,
            &config,
            our_share,
        )?);

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::{Configuration, Epoch, Sha3_256Digest, Threshold};

    use super::*;
    use assert_matches::assert_matches;
    use gfss::gf256::Gf256;
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

    #[test]
    pub fn handle_alarms() {
        let logctx = test_setup_log("handle_prepare_alarms");
        // Initial config
        let reconfig_msg = ReconfigureMsg {
            rack_id: RackUuid::new_v4(),
            epoch: Epoch(1),
            last_committed_epoch: None,
            members: (0..=5u8)
                .map(|serial| {
                    PlatformId::new("test".into(), serial.to_string())
                })
                .collect(),
            threshold: Threshold(3),
            retry_timeout: Duration::from_millis(100),
        };

        let my_platform_id = reconfig_msg.members.first().unwrap().clone();
        let mut node = Node::new(
            logctx.log.clone(),
            my_platform_id.clone(),
            PersistentState::empty(),
        );

        let mut outbox = Vec::new();
        let _ = node
            .coordinate_reconfiguration(
                Instant::now(),
                &mut outbox,
                reconfig_msg.clone(),
            )
            .expect("success")
            .expect("persistent state");

        // Configuration for our `PrepareMsg`
        let config = Configuration {
            rack_id: RackUuid::new_v4(),
            epoch: Epoch(1),
            coordinator: PlatformId::new("test".to_string(), "999".to_string()),
            members: (0..=5)
                .map(|serial| {
                    (
                        PlatformId::new("test".into(), serial.to_string()),
                        // Nonsense share
                        Sha3_256Digest([0u8; 32]),
                    )
                })
                .collect(),
            threshold: Threshold(3),
            previous_configuration: None,
        };
        let mut prepare_msg = PrepareMsg {
            // Generate a nonsense share
            share: Share {
                x_coordinate: Gf256::new(1),
                y_coordinates: (0..32_u8).map(Gf256::new).collect(),
            },
            config: config.clone(),
        };

        let alarm = node
            .handle(
                Instant::now(),
                &mut outbox,
                prepare_msg.config.coordinator.clone(),
                PeerMsg::Prepare(prepare_msg.clone()),
            )
            .unwrap_err();

        assert_matches!(
            alarm,
            Alarm::DifferentNodesCoordinatingSameEpoch { .. }
        );

        // Commit the initial configuration to better trigger alarms.
        node.commit_reconfiguration(reconfig_msg.epoch, reconfig_msg.rack_id)
            .unwrap();

        let alarm = node
            .handle(
                Instant::now(),
                &mut outbox,
                prepare_msg.config.coordinator.clone(),
                PeerMsg::Prepare(prepare_msg.clone()),
            )
            .unwrap_err();

        assert_matches!(alarm, Alarm::PrepareWithInvalidRackId { .. });

        // Set the rack_id to the correct one to allow other alarms to be
        // triggered.
        prepare_msg.config.rack_id = reconfig_msg.rack_id;
        let alarm = node
            .handle(
                Instant::now(),
                &mut outbox,
                prepare_msg.config.coordinator.clone(),
                PeerMsg::Prepare(prepare_msg.clone()),
            )
            .unwrap_err();

        assert_matches!(alarm, Alarm::PrepareLastCommittedEpochMismatch { .. });

        let alarm = node
            .commit_reconfiguration(Epoch(1), RackUuid::new_v4())
            .unwrap_err();
        assert_matches!(alarm, Alarm::CommitWithInvalidRackId { .. });

        let alarm = node
            .commit_reconfiguration(Epoch(3), reconfig_msg.rack_id)
            .unwrap_err();
        assert_matches!(alarm, Alarm::MissingPrepare { .. });

        let alarm = node
            .commit_reconfiguration(Epoch(0), reconfig_msg.rack_id)
            .unwrap_err();
        assert_matches!(alarm, Alarm::OutOfOrderCommit { .. });

        logctx.cleanup_successful();
    }
}
