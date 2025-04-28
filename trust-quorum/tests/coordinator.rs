// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Property based tests for the behavior of a reconfiguration coordinator

use assert_matches::assert_matches;
use bcs::Result;
use gfss::gf256::Gf256;
use gfss::shamir::Share;
use omicron_test_utils::dev::test_setup_log;
use omicron_uuid_kinds::RackUuid;
use prop::sample::Index;
use proptest::collection::btree_set;
use proptest::prelude::*;
use slog::Logger;
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};
use test_strategy::{Arbitrary, proptest};
use trust_quorum::{
    Configuration, EncryptedRackSecret, Envelope, Epoch, Node, PeerMsg,
    PersistentState, PlatformId, PrepareMsg, PreviousConfiguration,
    ReconfigureMsg, Salt, Sha3_256Digest, Threshold,
};

/// The system under test
pub struct Sut {
    // The coordinator node which is the system under test (SUT)
    pub node: Node,

    // The saved persistent state returned by the last Node operation
    pub persistent_state: PersistentState,
}

impl Sut {
    pub fn action_coordinate_reconfiguration(
        &mut self,
        now: Instant,
        outbox: &mut Vec<Envelope>,
        msg: ReconfigureMsg,
    ) -> Result<Option<PersistentState>, TestCaseError> {
        // We only generate valid configurations when calling this method. Any failure of
        // this method should be considered a test failure.
        let output = self.node.coordinate_reconfiguration(now, outbox, msg)?;
        Ok(output)
    }
}

/// An abstract model of our SUT
///
/// This allows us to build up a set of expected behaviors without reaching into
/// the SUT node itself to understand external events like messages that got
/// sent in response to a tick.
pub struct Model {
    // A copy of our id
    pub id: PlatformId,

    /// The current time
    pub now: Instant,

    /// The reconfiguration msg that triggered the last committed configuration
    pub last_committed_config_msg: Option<ReconfigureMsg>,

    /// A model of the SUT's `CoordinatorState`
    pub coordinator_state: Option<ModelCoordinatorState>,
}

impl Model {
    pub fn new(coordinator_id: PlatformId) -> Model {
        Model {
            id: coordinator_id,
            now: Instant::now(),
            last_committed_config_msg: None,
            coordinator_state: None,
        }
    }

    /// Advance time by `time_jump`.
    ///
    /// If the new time exceeds any deadline timers, reset those and return true.
    /// Otherwise return false.
    ///
    /// In the future we may wish to be specific about which timers expired.
    /// However, right now, there is only one.
    pub fn advance_time(&mut self, time_jump: Duration) -> bool {
        self.now += time_jump;
        if let Some(cs) = &mut self.coordinator_state {
            if self.now > cs.retry_deadline {
                cs.retry_deadline =
                    self.now + Duration::from_millis(RETRY_TIMEOUT_MS);
                return true;
            }
        }
        false
    }

    pub fn action_coordinate_reconfiguration(&mut self, msg: ReconfigureMsg) {
        // We don't currently collect LRTQ shares. This conditional
        // will have to be more specific when we do.
        let op = if self.last_committed_config_msg.is_some() {
            ModelCoordinatorOp::new_collect_shares(self.id.clone())
        } else {
            ModelCoordinatorOp::new_prepare(self.id.clone())
        };

        self.coordinator_state = Some(ModelCoordinatorState {
            msg,
            retry_deadline: self.now + Duration::from_millis(RETRY_TIMEOUT_MS),
            op,
        });
    }

    pub fn commit(&mut self, msg: ReconfigureMsg) {
        self.last_committed_config_msg = Some(msg);
        self.coordinator_state = None;
    }

    // If we are currently preparing this epoch then record the ack
    pub fn ack_prepare(&mut self, from: PlatformId, epoch: Epoch) {
        if let Some(cs) = &mut self.coordinator_state {
            if cs.msg.epoch == epoch {
                cs.op.ack_prepare(from);
            }
        }
    }

    // Delete the coordinator state
    pub fn stop_coordinating(&mut self) {
        self.coordinator_state = None;
    }

    /// If we are currently waiting for shares in this epoch then record the
    /// responder.
    ///
    /// If we've received enough shares to recompute the rack secret then
    /// transition the model operation to `Prepare` and return `Ok(true)`.
    pub fn handle_share(
        &mut self,
        from: PlatformId,
        epoch: Epoch,
    ) -> Result<bool, TestCaseError> {
        let Some(last_committed_config_msg) = &self.last_committed_config_msg
        else {
            return Ok(false);
        };
        let Some(cs) = &mut self.coordinator_state else {
            return Ok(false);
        };

        // Make sure our model state is consistent
        prop_assert_eq!(
            Some(last_committed_config_msg.epoch),
            cs.msg.last_committed_epoch
        );

        // Record the share
        if last_committed_config_msg.epoch == epoch {
            cs.op.handle_share(from);
        }

        // Do we have a threshold of shares? If so, we should transition our
        // operation to `Prepare`.
        if cs.op.collected_shares().unwrap_or(&BTreeSet::new()).len()
            == last_committed_config_msg.threshold.0 as usize
        {
            cs.op = ModelCoordinatorOp::new_prepare(self.id.clone());
            return Ok(true);
        }

        Ok(false)
    }

    /// Return nodes that have acked prepares for an ongoing coordination
    /// by the Model.
    ///
    /// Return `None` if the SUT is not currently coordinating or waiting on acks.
    pub fn acked_prepares(&self) -> Option<&BTreeSet<PlatformId>> {
        self.coordinator_state.as_ref().and_then(|cs| cs.op.acked_prepares())
    }

    /// Return the nodes that we are waiting for prepare acknowledgements from
    pub fn waiting_for_prepare_acks_from(
        &self,
    ) -> Option<BTreeSet<PlatformId>> {
        // We need to actually be coordinating to wait for shares
        let Some(cs) = &self.coordinator_state else {
            return None;
        };
        // We need to actually be waiting for acks
        let Some(acked_prepares) = self
            .coordinator_state
            .as_ref()
            .and_then(|cs| cs.op.acked_prepares())
        else {
            return None;
        };

        // Return all members of the current configuration that haven't acked a
        // `PrepareMsg` yet.
        Some(cs.msg.members.difference(acked_prepares).cloned().collect())
    }

    // Return the nodes that we are waiting for shares from
    pub fn waiting_for_shares_from(&self) -> Option<BTreeSet<PlatformId>> {
        // We only wait for shares if we have a prior committed config
        let Some(last_committed_config_msg) = &self.last_committed_config_msg
        else {
            return None;
        };
        // We need to actually be coordinating to wait for shares
        let Some(cs) = &self.coordinator_state else {
            return None;
        };
        // We need to actually be collecting shares as a coordinator
        let Some(collected_from) = cs.op.collected_shares() else {
            return None;
        };

        // Return all members of the last committed configuration that we
        // haven't received a share from yet.
        Some(
            last_committed_config_msg
                .members
                .difference(collected_from)
                .cloned()
                .collect(),
        )
    }

    // Return the current epoch being coordinated, if there is one
    fn coordinating_epoch(&self) -> Option<Epoch> {
        self.coordinator_state.as_ref().map(|cs| cs.msg.epoch)
    }

    /// Return the members of the current reconfiguration being coordinated.
    ///
    /// Return the empty set if there is no ongoing coordination.
    pub fn active_reconfiguration_members(&self) -> BTreeSet<PlatformId> {
        self.coordinator_state
            .as_ref()
            .map(|cs| cs.msg.members.clone())
            .unwrap_or(BTreeSet::new())
    }
}

/// A model of the SUT's internal `CoordinatorState`
pub struct ModelCoordinatorState {
    /// The reconfiguration msg that triggered the current configuration
    pub msg: ReconfigureMsg,
    pub retry_deadline: Instant,
    pub op: ModelCoordinatorOp,
}

/// A model of the SUT's internal `CoordinatorOp`
pub enum ModelCoordinatorOp {
    CollectShares { collected_from: BTreeSet<PlatformId> },
    CollectLrtqShares { collected_from: BTreeSet<PlatformId> },
    Prepare { acked: BTreeSet<PlatformId> },
}

impl ModelCoordinatorOp {
    pub fn new_collect_shares(ourself: PlatformId) -> Self {
        ModelCoordinatorOp::CollectShares {
            // Always include ourself
            collected_from: BTreeSet::from([ourself]),
        }
    }

    pub fn new_prepare(ourself: PlatformId) -> Self {
        ModelCoordinatorOp::Prepare {
            // Always include ourself
            acked: BTreeSet::from([ourself]),
        }
    }

    pub fn ack_prepare(&mut self, from: PlatformId) {
        if let ModelCoordinatorOp::Prepare { acked } = self {
            acked.insert(from);
        }
    }

    pub fn acked_prepares(&self) -> Option<&BTreeSet<PlatformId>> {
        if let ModelCoordinatorOp::Prepare { acked } = self {
            Some(acked)
        } else {
            None
        }
    }

    pub fn handle_share(&mut self, from: PlatformId) {
        if let ModelCoordinatorOp::CollectShares { collected_from } = self {
            collected_from.insert(from);
        }
    }

    pub fn collected_shares(&self) -> Option<&BTreeSet<PlatformId>> {
        if let ModelCoordinatorOp::CollectShares { collected_from } = self {
            Some(collected_from)
        } else {
            None
        }
    }

    pub fn is_collecting_shares(&self) -> bool {
        matches!(self, ModelCoordinatorOp::CollectShares { .. })
    }

    pub fn is_collecting_lrtq_shares(&self) -> bool {
        matches!(self, ModelCoordinatorOp::CollectLrtqShares { .. })
    }

    pub fn is_preparing(&self) -> bool {
        matches!(self, ModelCoordinatorOp::Prepare { .. })
    }
}

// The test itself maintains an internal state that includes not only the state
// of the `Node`, but also enough state of the trust quorum system to enable
// tests via `Action`s.
struct TestState {
    /// Our system under test
    sut: Sut,

    /// The abstract model of our SUT
    model: Model,

    // All in flight messages to nodes that are not the SUT
    network_msgs: BTreeMap<PlatformId, Vec<PeerMsg>>,

    // Messages delivered to each node that is not the SUT
    delivered_msgs: BTreeMap<PlatformId, Vec<PeerMsg>>,

    // Shares delivered to nodes that are not the SUT.
    //
    // We track these so that we can implement replies for `GetShare` messages.
    delivered_shares: BTreeMap<(Epoch, PlatformId), Share>,

    // A cache of our member universe, so we only have to generate it once
    member_universe: Vec<PlatformId>,

    // The last epoch for a `PrepareMsg` sent by Nexus
    // In production this is stored in CRDB, but it is simulated here
    highest_prepared_epoch: Epoch,

    // We reuse the same coordinator ID across all test runs
    // There's no reason to randomize it.
    coordinator_id: PlatformId,

    // No reason to change the rack_id
    rack_id: RackUuid,
}

impl TestState {
    pub fn new(log: Logger) -> TestState {
        let member_universe = member_universe();
        let coordinator_id = member_universe[0].clone();
        TestState {
            sut: Sut {
                node: Node::new(
                    log,
                    coordinator_id.clone(),
                    PersistentState::empty(),
                ),
                persistent_state: PersistentState::empty(),
            },
            model: Model::new(coordinator_id.clone()),
            network_msgs: BTreeMap::new(),
            delivered_msgs: BTreeMap::new(),
            delivered_shares: BTreeMap::new(),
            member_universe,
            highest_prepared_epoch: Epoch(0),
            coordinator_id,
            rack_id: RackUuid::new_v4(),
        }
    }

    pub fn action_coordinate_reconfiguration(
        &mut self,
        generated_config: GeneratedConfiguration,
    ) -> Result<(), TestCaseError> {
        let mut outbox = Vec::new();

        let msg = self.generated_config_to_reconfigure_msg(generated_config);

        // Update the model state
        self.model.action_coordinate_reconfiguration(msg.clone());

        // Update the SUT state
        //
        // We only generate valid configurations when calling this method. Any failure of
        // this method should be considered a test failure.
        let output = self.sut.action_coordinate_reconfiguration(
            self.model.now,
            &mut outbox,
            msg,
        )?;

        match output {
            Some(persistent_state) => {
                // The request succeeded
                self.assert_persistent_state_after_coordinate_reconfiguration(
                    &persistent_state,
                )?;

                // We validated our persistent state is correct. Save it and move
                // on.
                self.sut.persistent_state = persistent_state;

                // The correct messages were sent
                self.assert_envelopes_after_coordinate_reconfiguration(
                    &outbox,
                )?;

                // We validated our messages. Let's put them into our test state as "in-flight".
                self.send(outbox.into_iter());
            }
            None => {
                // This is a reconfiguration, so before a `PersistentState`
                // is returned we must collect share for the last committed
                // configuration.
                self.assert_envelopes_after_coordinate_reconfiguration(
                    &outbox,
                )?;

                // We validated our messages. Let's put them into our test state as "in-flight".
                self.send(outbox.into_iter());
            }
        }

        Ok(())
    }

    // Execute the proptest generated actions
    pub fn run_actions(
        &mut self,
        actions: Vec<Action>,
    ) -> Result<(), TestCaseError> {
        for action in actions {
            match action {
                Action::DeliverMsgs(indices) => {
                    self.action_deliver_msgs(indices);
                }
                Action::DropMessages(indices) => {
                    self.action_drop_msgs(indices);
                }
                Action::Reply(indices) => {
                    self.action_reply(indices)?;
                }
                Action::Tick(time_jump) => {
                    self.action_tick(time_jump)?;
                }
                Action::Commit(index) => {
                    self.action_commit(index)?;
                }
                Action::CoordinateReconfiguration(generated_config) => {
                    self.action_coordinate_reconfiguration(generated_config)?;
                }
                Action::SendPrepareToSut(generated_config) => {
                    self.action_send_prepare_to_sut(generated_config)?;
                }
            }
        }
        Ok(())
    }

    // Deliver network messages to generated destinations
    fn action_deliver_msgs(&mut self, indices: Vec<Index>) {
        let destinations: Vec<_> = self.network_msgs.keys().cloned().collect();
        if destinations.is_empty() {
            // nothing to do
            return;
        }
        for index in indices {
            let id = index.get(&destinations);
            if let Some(msg) = self.network_msgs.get_mut(id).unwrap().pop() {
                // If the message is a `Prepare`, also save its share so we can
                // reply to `GetShare` messages.
                if let PeerMsg::Prepare(prepare) = &msg {
                    self.delivered_shares.insert(
                        (prepare.config.epoch, id.clone()),
                        prepare.share.clone(),
                    );
                }
                let msgs = self.delivered_msgs.entry(id.clone()).or_default();
                msgs.push(msg);
            }
        }

        // Remove any destinations with zero messages in-flight
        self.network_msgs.retain(|_, msgs| !msgs.is_empty());
    }

    // Drop network messages destined for generated destinations
    fn action_drop_msgs(&mut self, indices: Vec<Index>) {
        let destinations: Vec<_> = self.network_msgs.keys().cloned().collect();
        if destinations.is_empty() {
            // nothing to do
            return;
        }
        for index in indices {
            let id = index.get(&destinations);
            let _ = self.network_msgs.get_mut(id).unwrap().pop();
        }

        // Remove any destinations with zero messages in-flight
        self.network_msgs.retain(|_, msgs| !msgs.is_empty());
    }

    /// Send replies from nodes with delivered messages
    fn action_reply(
        &mut self,
        indices: Vec<Index>,
    ) -> Result<(), TestCaseError> {
        let sources: Vec<_> = self.delivered_msgs.keys().cloned().collect();
        if sources.is_empty() {
            // nothing to do
            return Ok(());
        }
        for index in indices {
            let from = index.get(&sources);

            // We don't bother floating replies in the network. We immediately
            // return them to the coordinator.
            if let Some(msg) = self.delivered_msgs.get_mut(from).unwrap().pop()
            {
                match msg {
                    PeerMsg::Prepare(prepare_msg) => {
                        self.reply_to_prepare_msg(from.clone(), prepare_msg)?;
                    }
                    PeerMsg::GetShare(epoch) => {
                        self.reply_to_get_share_msg(from.clone(), epoch)?;
                    }
                    _ => todo!(),
                }
            }
        }

        // Remove any nodes with zero delivered messages
        self.delivered_msgs.retain(|_, msgs| !msgs.is_empty());

        Ok(())
    }

    fn action_send_prepare_to_sut(
        &mut self,
        generated_config: GeneratedConfiguration,
    ) -> Result<(), TestCaseError> {
        // It doesn't really matter what the configuration looks like, as long
        // as the last committed epoch matches what the SUT has, and the epoch
        // of the `Prepare` is greater than the largest seen epoch at the SUT.
        // Therefore, we are able to use our existing config generation mechanism,
        // as we do with the SUT.
        let msg = self.generated_config_to_reconfigure_msg(generated_config);

        // Choose a node to send to the SUT.
        // Skip over the SUT which sorts first.
        let coordinator = msg.members.iter().skip(1).next().cloned().unwrap();

        // Since the test is never going to commit this Prepare, we just use
        // dummy values where necessary (primarily when crypto is involved).
        let config = Configuration {
            rack_id: msg.rack_id,
            epoch: msg.epoch,
            coordinator: coordinator.clone(),
            members: msg
                .members
                .into_iter()
                .map(|id| (id, Sha3_256Digest([0u8; 32])))
                .collect(),
            threshold: msg.threshold,
            previous_configuration: msg.last_committed_epoch.map(|epoch| {
                PreviousConfiguration {
                    epoch,
                    is_lrtq: false,
                    encrypted_last_committed_rack_secret: EncryptedRackSecret(
                        vec![0u8; 32],
                    ),
                    encrypted_last_committed_rack_secret_salt: Salt::new(),
                }
            }),
        };
        // Generate a nonsense share for the SUT
        let share = Share {
            x_coordinate: Gf256::new(1),
            y_coordinates: (0..32 as u8).into_iter().map(Gf256::new).collect(),
        };

        let prepare_msg = PrepareMsg { config, share };

        // Put the model in the correct state
        self.model.stop_coordinating();

        // Handle the `PrepareMsg` at the SUT
        let mut outbox = Vec::new();
        let persistent_state = self
            .sut
            .node
            .handle(
                self.model.now,
                &mut outbox,
                coordinator.clone(),
                PeerMsg::Prepare(prepare_msg.clone()),
            )
            .expect("persistent state");

        // We should have gotten back a persistent state including the prepare
        // received by the SUT.
        prop_assert_eq!(
            self.sut.persistent_state.prepares.len() + 1,
            persistent_state.prepares.len()
        );
        prop_assert_eq!(
            &prepare_msg.config,
            &persistent_state.prepares.get(&msg.epoch).unwrap().config,
        );

        // The SUT should have replied with an ack
        prop_assert_eq!(outbox.len(), 1);
        let envelope = outbox.pop().expect("prepare ack");
        prop_assert_eq!(envelope.to, coordinator);
        prop_assert_eq!(&envelope.from, self.sut.node.platform_id());
        assert_matches!(envelope.msg, PeerMsg::PrepareAck(epoch) => {
            prop_assert_eq!(epoch, msg.epoch);
        });

        // The SUT should no longer be coordinating
        prop_assert!(self.sut.node.get_coordinator_state().is_none());

        // All our assertions passed. Update the SUT's persistent state.
        self.sut.persistent_state = persistent_state.clone();

        Ok(())
    }

    fn action_tick(
        &mut self,
        time_jump: Duration,
    ) -> Result<(), TestCaseError> {
        let mut outbox = Vec::new();

        // Tell our model and the SUT that time has advanced
        let timer_expired = self.model.advance_time(time_jump);
        self.sut.node.tick(self.model.now, &mut outbox);

        // If time has advanced past the coordinator's retry deadline
        // then we must see if we expected any retries to be sent.
        if timer_expired {
            self.assert_expected_outgoing_envelopes(&outbox)?;
        }

        // Put any output messages onto the network
        self.send(outbox.into_iter());

        Ok(())
    }

    fn assert_expected_outgoing_envelopes(
        &self,
        outbox: &[Envelope],
    ) -> Result<(), TestCaseError> {
        if let Some(expected) = self.model.waiting_for_prepare_acks_from() {
            for envelope in outbox {
                assert_matches!(envelope.msg, PeerMsg::Prepare(_));
                prop_assert!(expected.contains(&envelope.to));
            }
            // We are only sending either `Prepare` or `GetShare` variants, not both.
            return Ok(());
        }

        if let Some(expected) = self.model.waiting_for_shares_from() {
            for envelope in outbox {
                assert_matches!(envelope.msg, PeerMsg::GetShare(_));
                prop_assert!(expected.contains(&envelope.to));
            }
        }

        Ok(())
    }

    // Precondition: Nexus will only actually commit if enough nodes
    // have acked the `PrepareMsg` from the coordinator. Let's simulate
    // that by checking our model state.
    //
    // Return `Some(reconfigure_msg)` if Nexus would decide to commit, `None` otherwise.
    fn action_commit_precondition(
        &mut self,
        index: Index,
    ) -> Option<ReconfigureMsg> {
        // If we aren't currently coordinating, then just return as there's no
        // configuration to commit.
        let Some(cs) = &self.model.coordinator_state else {
            return None;
        };

        // Compute the safety threshold used by nexus
        // Nexus will choose a constant, but we vary it because we can.
        let max_possible_z = cs.msg.members.len() - cs.msg.threshold.0 as usize;
        let z =
            if max_possible_z == 0 { 0 } else { index.index(max_possible_z) };

        // We can only commit when the coordinator has seen a `PrepareAck` from
        // K + Z nodes.
        //
        // Justification:  The coordinator does not know what `Z` is and cannot
        // know if it has seen enough acks to know if it's safe to commit, so
        // nexus must not try. Furthermore, Nexus informs non-coordinator nodes
        // when to commit and they have no way to know if enough nodes have
        // shares as a result of receiving `PrepareMsg`s. Because of this, we
        // rely on Nexus to reliably uphold this correctness invariant, and
        // simulate it in the test.
        let acked = cs.op.acked_prepares().map(|s| s.len()).unwrap_or(0);
        if acked >= cs.msg.threshold.0 as usize + z {
            Some(cs.msg.clone())
        } else {
            None
        }
    }

    fn action_commit(&mut self, index: Index) -> Result<(), TestCaseError> {
        let Some(msg) = self.action_commit_precondition(index) else {
            // We failed our precondition check. This isn't a test failure. It
            // just means that the coordinator is not ready to commit.
            return Ok(());
        };

        // Commit the configuration at our node
        let persistent_state = self
            .sut
            .node
            .commit_reconfiguration(msg.epoch, msg.rack_id)?
            .expect("persistent state has been updated");

        // Check the output of the persistent state
        self.assert_persistent_state_after_commit_reconfiguration(
            msg.epoch,
            &persistent_state,
        )?;

        // Save the persistent state
        self.sut.persistent_state = persistent_state;

        // Update our model state
        self.model.commit(msg);

        Ok(())
    }

    fn generated_config_to_reconfigure_msg(
        &mut self,
        config: GeneratedConfiguration,
    ) -> ReconfigureMsg {
        let mut members: BTreeSet<PlatformId> = config
            .members
            .iter()
            .map(|index| self.member_universe[*index].clone())
            .collect();

        // Ensure that the configuration always includes the coordinator
        members.insert(self.coordinator_id.clone());

        let threshold = Threshold(usize::max(
            2,
            config.threshold.index(members.len()),
        ) as u8);

        // Increment the epoch for this configuration. This simulates what Nexus
        // would do.
        self.highest_prepared_epoch = self.highest_prepared_epoch.inc();

        ReconfigureMsg {
            rack_id: self.rack_id,
            epoch: self.highest_prepared_epoch,
            last_committed_epoch: self
                .model
                .last_committed_config_msg
                .as_ref()
                .map(|msg| msg.epoch),
            members,
            threshold,
            retry_timeout: Duration::from_millis(RETRY_TIMEOUT_MS),
        }
    }

    fn reply_to_prepare_msg(
        &mut self,
        from: PlatformId,
        msg: PrepareMsg,
    ) -> Result<(), TestCaseError> {
        // We always reply to a `Prepare` with a `PrepareAck`.
        //
        // We are testing the coordinator, and assume it is sending
        // `Prepares` that are up to date, and expecting back appropriate acks.
        //
        // In any case, we don't keep enough state at the fake follower replicas
        // to check this.
        let reply = PeerMsg::PrepareAck(msg.config.epoch);
        let mut outbox = Vec::new();
        let output = self.sut.node.handle(
            self.model.now,
            &mut outbox,
            from.clone(),
            reply,
        );
        prop_assert!(output.is_none());
        prop_assert!(outbox.is_empty());

        // Also update the model state
        self.model.ack_prepare(from.clone(), msg.config.epoch);

        // Ensure that if the SUT is waiting for prepares, that the ack
        // is accounted for.
        //
        // We only perform this check if this reply is for the current epoch
        if self.model.coordinating_epoch() == Some(msg.config.epoch) {
            if let Some(acks) = self.model.acked_prepares() {
                prop_assert!(acks.contains(&from));
            }
        }

        Ok(())
    }

    fn reply_to_get_share_msg(
        &mut self,
        from: PlatformId,
        epoch: Epoch,
    ) -> Result<(), TestCaseError> {
        // If we have a share, then return it. Otherwise return nothing.
        // In the future we may implement other behaviors.
        let Some(share) = self.delivered_shares.get(&(epoch, from.clone()))
        else {
            return Ok(());
        };
        let reply = PeerMsg::Share { epoch, share: share.clone() };
        let mut outbox = Vec::new();
        let output = self.sut.node.handle(
            self.model.now,
            &mut outbox,
            from.clone(),
            reply,
        );

        // If we just received a threshold number of shares, we expect
        // reconstruction of the rack secret for the last committed
        // configuration and the SUT to start sending prepare messages. As part
        // of this transition the persistent state with the SUT's own latest
        // `PrepareMsg` is also returned.
        let start_preparing = self.model.handle_share(from.clone(), epoch)?;
        if start_preparing {
            // We just transitioned to preparing
            let Some(persistent_state) = output else {
                return Err(TestCaseError::fail(
                    "Persistent state should exist",
                ));
            };

            // The same checks should hold.
            // TODO: Perhaps we should rename this method.
            self.assert_persistent_state_after_coordinate_reconfiguration(
                &persistent_state,
            )?;

            // We validated our persistent state is correct. Save it and move
            // on.
            self.sut.persistent_state = persistent_state;
        }

        self.assert_expected_outgoing_envelopes(&outbox)?;

        self.send(outbox.into_iter());

        Ok(())
    }

    /// Ensure that the output of `Node::commit_reconfiguration` is valid
    /// given the test state.
    fn assert_persistent_state_after_commit_reconfiguration(
        &self,
        epoch: Epoch,
        persistent_state: &PersistentState,
    ) -> Result<(), TestCaseError> {
        // We should have one new commit for epoch: Epoch
        prop_assert_eq!(
            self.sut.persistent_state.commits.len() + 1,
            persistent_state.commits.len()
        );
        assert!(!self.sut.persistent_state.commits.contains(&epoch));
        assert!(persistent_state.commits.contains(&epoch));

        // The SUT node should no longer be coordinating
        assert!(self.sut.node.get_coordinator_state().is_none());

        Ok(())
    }

    /// Ensure that the output of `Node::coordinate_reconfiguration`
    /// is valid given the `TestState`.
    ///
    /// This is essentially a "postcondition" check.
    fn assert_persistent_state_after_coordinate_reconfiguration(
        &self,
        persistent_state: &PersistentState,
    ) -> Result<(), TestCaseError> {
        let sut = &self.sut;
        let msg = &self
            .model
            .coordinator_state
            .as_ref()
            .ok_or(TestCaseError::fail(
                "Model should have a coordinator state",
            ))?
            .msg;

        // TODO: This will have to change once we start supporting LRTQ
        prop_assert!(persistent_state.lrtq.is_none());
        prop_assert_eq!(
            &sut.persistent_state.commits,
            &persistent_state.commits
        );
        prop_assert!(persistent_state.decommissioned.is_none());
        prop_assert_eq!(
            sut.persistent_state.prepares.len() + 1,
            persistent_state.prepares.len()
        );

        prop_assert_eq!(
            persistent_state.last_prepared_epoch().unwrap(),
            msg.epoch
        );

        let config = persistent_state.configuration(msg.epoch).unwrap();
        prop_assert_eq!(config.epoch, msg.epoch);
        for member in config.members.keys() {
            prop_assert!(msg.members.contains(member));
        }
        prop_assert_eq!(config.threshold, msg.threshold);

        prop_assert_eq!(
            config.previous_configuration.as_ref().map(|c| c.epoch),
            msg.last_committed_epoch
        );

        Ok(())
    }

    /// Verify the expected messages are sent after calling
    /// `Node::coordinate_reconfiguration`.
    fn assert_envelopes_after_coordinate_reconfiguration(
        &self,
        outbox: &[Envelope],
    ) -> Result<(), TestCaseError> {
        let sut = &self.sut;
        let model_cs = &self.model.coordinator_state.as_ref().ok_or(
            TestCaseError::fail("Model should have a coordinator state"),
        )?;

        // The coordinator should send messages to every node but itself
        for envelope in outbox {
            if model_cs.op.is_preparing() {
                // We should be sending to every member of the current config
                // except ourself.
                assert_eq!(outbox.len(), model_cs.msg.members.len() - 1);

                // Safety: We'll always have this configuration in the SUT persistent
                // state if we are preparing.
                let config = sut
                    .persistent_state
                    .configuration(model_cs.msg.epoch)
                    .expect("config for this epoch: {epoch}");

                assert_matches!(
                    &envelope.msg,
                    PeerMsg::Prepare(PrepareMsg { config: prepare_config, .. }) => {
                        assert_eq!(*config, *prepare_config);
                    }
                );
            } else {
                // We should be sending to every member of the previous config
                // except ourself.
                //
                // Safety: If we got here, then we have a prior committed
                // configuration.
                assert_eq!(
                    outbox.len(),
                    self.model
                        .last_committed_config_msg
                        .as_ref()
                        .unwrap()
                        .members
                        .len()
                        - 1
                );
                assert_matches!(
                    &envelope.msg,
                    PeerMsg::GetShare(epoch) => {
                        assert_eq!(*epoch, model_cs.msg.last_committed_epoch.unwrap());
                    }
                );
            }
            prop_assert_eq!(&envelope.from, &self.coordinator_id);

            // We don't send a prepare to ourselves. We put it in our
            // `PersistentState` directly.
            prop_assert_ne!(&envelope.to, &self.coordinator_id);
        }

        Ok(())
    }

    fn send(&mut self, envelopes: impl Iterator<Item = Envelope>) {
        for envelope in envelopes {
            let msgs = self.network_msgs.entry(envelope.to).or_default();
            msgs.push(envelope.msg);
        }
    }
}

// A high-level set of actions to drive the state of a peer forward.
#[derive(Debug, Arbitrary)]
pub enum Action {
    /// For each indexed member deliver an in-flight network msg if there is
    /// one.
    ///
    /// The indexes here are used to index into the `PlatformIds` of
    /// `test_state.network_msgs`.
    ///
    /// We may deliver more than one message to each member.
    #[weight(50)]
    DeliverMsgs(Vec<Index>),

    /// For each indexed member drop an in-flight network msg if there is
    /// one.
    ///
    /// The indexes here are used to index into the `PlatformIds` of
    /// `test_state.network_msgs`.
    ///
    /// We may drop more than one message to each member, which is why we
    /// use a Vec. We don't worry about shuffling messsages so they are out of
    /// order, because we use stream oriented transport.
    //#[weight(3)]
    DropMessages(Vec<Index>),

    /// Fake a reply for a delivered message.
    ///
    /// The indexes here are used to index into the `PlatformIds` of
    /// `test_state.delivered_msgs`.
    #[weight(45)]
    Reply(Vec<Index>),

    // Start a reconfiguration that has a possibility of succeeding
    // given delivered messages.
    //
    // It will never fail a validation check.
    //#[weight(2)]
    CoordinateReconfiguration(GeneratedConfiguration),

    // A tick duration in milliseconds
    #[weight(50)]
    Tick(
        #[strategy(
            (RETRY_TIMEOUT_MS/4..RETRY_TIMEOUT_MS*5/4)
            .prop_map(Duration::from_millis)
         )]
        Duration,
    ),

    // Attempt to Commit a reconfiguration if possible
    //
    // We'll only attempt to commit a reconfiguration if K+Z nodes
    // have acked prepares for that epoch.
    //
    // Z is randomly chosen between 0 and N-K, and is a "safety parameter"
    // described in RFD 238.
    //
    // `Index` is used to compute Z here.
    #[weight(1)]
    Commit(Index),

    // Send a `PrepareMsg` from a fake test node, indicating that Nexus has told
    // that node to start coordinating a new reconfiguration . This should cause
    // the the `SUT` to stop coordinating if it is currently coordinating.
    //
    // We always send a `PrepareMsg` with an epoch that is newer than the latest
    // configuration, as the goal is for the request to be accepted by the SUT.
    #[weight(1)]
    SendPrepareToSut(GeneratedConfiguration),
}

/// Informnation about configurations used at test generation time
#[derive(Debug, Clone, Arbitrary)]
pub struct GeneratedConfiguration {
    /// The members of this configuration
    ///
    /// We don't use an `Index` here because we know we have a fixed size
    /// universe to select from, and we want to guarantee uniqueness. If we
    /// derived `Ord` on `Index` and stored them in a `BTreeSet`, we'd get a
    /// unique set of `Index` values. But when used, the output indexes can
    /// still be duplicated due to the shift implementation used. Therefore we
    /// instead just choose from a constrained set of usize values that we can
    /// use directly as indexes into our fixed size structure for all tests.
    ///
    /// Note that we intentionally set the max set size to MAX_CLUSTER_SIZE-1.
    /// This is because we always want to include the coordinator in the
    /// configuration, but it's value may not be chosen randomly. In this case,
    /// we have to add it to the actual membership set we generate from this
    /// configuration with [`TestState::generated_config_to_reconfigure_msg`].
    #[strategy(btree_set(0..=255usize, MIN_CLUSTER_SIZE..MAX_CLUSTER_SIZE))]
    pub members: BTreeSet<usize>,

    /// An index is roughly equivalent to a threshold, since a threshold cannot
    /// exceed the number of members in a configuration. Using the index is
    /// very efficient as opposed to a `prop_flat_map`. When we calculate the
    /// threshold from the index we use max(2, Index), since the minimum
    /// threshold is always 2.
    pub threshold: Index,
}

/// All possible members used in a test
fn member_universe() -> Vec<PlatformId> {
    (0..=255)
        .map(|serial| PlatformId::new("test".into(), serial.to_string()))
        .collect()
}

const MIN_CLUSTER_SIZE: usize = 3;
const MAX_CLUSTER_SIZE: usize = 32;
const RETRY_TIMEOUT_MS: u64 = 100;

#[derive(Debug, Arbitrary)]
pub struct TestInput {
    initial_config: GeneratedConfiguration,
    actions: Vec<Action>,
}

#[proptest]
fn test_coordinator_behavior_from_empty_state(input: TestInput) {
    let logctx = test_setup_log("coordinator_behavior_from_empty_state");

    let mut state = TestState::new(logctx.log.clone());

    // Start the initial configuration
    state.action_coordinate_reconfiguration(input.initial_config)?;

    // Start executing the actions
    state.run_actions(input.actions)?;

    logctx.cleanup_successful();
}
