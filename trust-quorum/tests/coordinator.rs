// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Property based tests for the behavior of a reconfiguration coordinator

use assert_matches::assert_matches;
use bcs::Result;
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
    Envelope, Epoch, Node, PeerMsg, PeerMsgKind, PersistentState, PlatformId,
    ReconfigureMsg, Threshold,
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
        // We only generate valid configurations when calling this method. Any
        // failure of this method should be considered a test failure.
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
    /// The current time
    pub now: Instant,

    /// The reconfiguration msg that triggered the last committed configuration
    pub last_committed_config_msg: Option<ReconfigureMsg>,

    /// A model of the SUT's `CoordinatorState`
    pub coordinator_state: Option<ModelCoordinatorState>,
}

impl Model {
    pub fn new() -> Model {
        Model {
            now: Instant::now(),
            last_committed_config_msg: None,
            coordinator_state: None,
        }
    }

    /// Advance time by `time_jump`.
    ///
    /// If the new time exceeds any deadline timers, reset those and return
    /// true. Otherwise return false.
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
            ModelCoordinatorOp::CollectShares {
                collected_from: BTreeSet::new(),
            }
        } else {
            ModelCoordinatorOp::Prepare { acked: BTreeSet::new() }
        };

        self.coordinator_state = Some(ModelCoordinatorState {
            msg,
            retry_deadline: self.now + Duration::from_millis(RETRY_TIMEOUT_MS),
            op,
        });
    }

    // If we are currently preparing this epoch then record the ack
    pub fn ack_prepare(&mut self, from: PlatformId, epoch: Epoch) {
        if let Some(cs) = &mut self.coordinator_state {
            if cs.msg.epoch == epoch {
                cs.op.ack_prepare(from);
            }
        }
    }

    /// Return nodes that have acked prepares for an ongoing coordination
    /// by the Model.
    ///
    /// Return `None` if the SUT is not currently coordinating or waiting on
    /// acks.
    pub fn acked_prepares(&self) -> Option<&BTreeSet<PlatformId>> {
        self.coordinator_state.as_ref().and_then(|cs| cs.op.acked_prepares())
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
}

// The test itself maintains an internal state that includes not only the state
// of the `Node`, but also enough state of the trust quorum system to enable
// tests via `Action`s.
struct TestState {
    /// Our system under test
    pub sut: Sut,

    /// The abstract model of our SUT
    pub model: Model,

    // All in flight messages to nodes that are not the SUT
    pub network_msgs: BTreeMap<PlatformId, Vec<PeerMsg>>,

    // Messages delivered to each node that is not the SUT
    pub delivered_msgs: BTreeMap<PlatformId, Vec<PeerMsg>>,
}

impl TestState {
    pub fn new(log: Logger, coordinator_id: PlatformId) -> TestState {
        TestState {
            sut: Sut {
                node: Node::new(log, coordinator_id, PersistentState::empty()),
                persistent_state: PersistentState::empty(),
            },
            model: Model::new(),
            network_msgs: BTreeMap::new(),
            delivered_msgs: BTreeMap::new(),
        }
    }

    pub fn action_coordinate_reconfiguration(
        &mut self,
        msg: ReconfigureMsg,
    ) -> Result<(), TestCaseError> {
        let mut outbox = Vec::new();

        // Update the model state
        self.model.action_coordinate_reconfiguration(msg.clone());

        // Update the SUT state
        //
        // We only generate valid configurations when calling this method. Any
        // failure of this method should be considered a test failure.
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

                // We validated our persistent state is correct. Save it and
                // move on.
                self.sut.persistent_state = persistent_state;

                // The correct messages were sent
                self.assert_envelopes_after_coordinate_reconfiguration(
                    &outbox,
                )?;

                // We validated our messages. Let's put them into our test state
                // as "in-flight".
                self.send(outbox.into_iter());
            }
            None => {
                // The request is idempotent
                // No action should have been taken
                prop_assert!(outbox.is_empty());
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
                Action::Reply(indices) => {
                    self.action_reply(indices)?;
                }
                Action::Tick(time_jump) => {
                    self.action_tick(time_jump)?;
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
                let msgs = self.delivered_msgs.entry(id.clone()).or_default();
                msgs.push(msg);
            }
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
                    PeerMsg {
                        rack_id,
                        kind: PeerMsgKind::Prepare { config, .. },
                    } => {
                        self.reply_to_prepare_msg(
                            from.clone(),
                            rack_id,
                            config.epoch,
                        )?;
                    }
                    _ => todo!(),
                }
            }
        }

        // Remove any nodes with zero delivered messages
        self.delivered_msgs.retain(|_, msgs| !msgs.is_empty());

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
            // Get the members of the current configuration being coordinated
            let members = self.model.active_reconfiguration_members();

            // We aren't coordinating
            if members.is_empty() {
                prop_assert!(outbox.is_empty());
                return Ok(());
            }

            if let Some(acked_members) = self.model.acked_prepares() {
                // We expect retries for all members that the coordinator
                // has not received acks for.
                let expected: BTreeSet<_> =
                    members.difference(acked_members).collect();
                for envelope in &outbox {
                    prop_assert!(expected.contains(&envelope.to));
                }
            } else {
                // We aren't waiting on acks, so won't retry sending prepares
                prop_assert!(outbox.is_empty());
            }
        }

        // Put any output messages onto the network
        self.send(outbox.into_iter());

        Ok(())
    }

    fn reply_to_prepare_msg(
        &mut self,
        from: PlatformId,
        rack_id: RackUuid,
        epoch: Epoch,
    ) -> Result<(), TestCaseError> {
        // We always reply to a `Prepare` with a `PrepareAck`.
        //
        // We are testing the coordinator, and assume it is sending
        // `Prepares` that are up to date, and expecting back appropriate acks.
        //
        // In any case, we don't keep enough state at the fake follower replicas
        // to check this.
        let reply = PeerMsg { rack_id, kind: PeerMsgKind::PrepareAck(epoch) };
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
        self.model.ack_prepare(from.clone(), epoch);

        // Ensure that if the SUT is waiting for prepares, that the ack
        // is accounted for.
        if let Some(acks) = self.model.acked_prepares() {
            prop_assert!(acks.contains(&from));
        }

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

        prop_assert!(persistent_state.lrtq.is_none());
        prop_assert_eq!(
            &sut.persistent_state.commits,
            &persistent_state.commits
        );
        prop_assert!(persistent_state.expunged.is_none());
        prop_assert_eq!(
            sut.persistent_state.configs.len() + 1,
            persistent_state.configs.len()
        );

        prop_assert_eq!(
            persistent_state.latest_config().unwrap().epoch,
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
        let msg = &self
            .model
            .coordinator_state
            .as_ref()
            .ok_or(TestCaseError::fail(
                "Model should have a coordinator state",
            ))?
            .msg;

        let config = sut.persistent_state.configuration(msg.epoch).unwrap();

        // Ensure the members of the configuration match the model msg
        prop_assert_eq!(
            &msg.members,
            &config.members.keys().cloned().collect()
        );

        // The coordinator should send messages to every node but itself
        assert_eq!(outbox.len(), config.members.len() - 1);
        for envelope in outbox {
            assert_matches!(
                &envelope.msg,
                PeerMsg{
                    kind: PeerMsgKind::Prepare{config: prepare_config, .. },
                    ..} =>
                {
                    assert_eq!(*config, *prepare_config);
                }
            );
            prop_assert_eq!(&envelope.from, &config.coordinator);

            // We don't send a prepare to ourselves. We put it in our
            // `PersistentState` directly.
            prop_assert_ne!(&envelope.to, &config.coordinator);
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
    //DropMessages(Vec<Index>),

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
    //StartReconfiguration(GeneratedConfiguration),

    // A tick duration in milliseconds
    #[weight(50)]
    Tick(
        #[strategy(
            (RETRY_TIMEOUT_MS/4..RETRY_TIMEOUT_MS*5/4)
            .prop_map(Duration::from_millis)
         )]
        Duration,
    ),
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
    #[strategy(btree_set(0..=255usize, MIN_CLUSTER_SIZE..=MAX_CLUSTER_SIZE))]
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

    let universe = member_universe();
    let rack_id = RackUuid::new_v4();

    // We don't know the coordinator ID until we translate the indexes in the
    // test input to `PlatformId`s.
    let coordinator_id = None;
    let initial_config_msg = generated_config_to_reconfigure_msg(
        &universe,
        coordinator_id,
        Epoch(1),
        rack_id,
        input.initial_config,
    );

    let coordinator_id = initial_config_msg.members.first().unwrap().clone();
    let mut state = TestState::new(logctx.log.clone(), coordinator_id);

    // Start the initial configuration
    state.action_coordinate_reconfiguration(initial_config_msg)?;

    // Start executing the actions
    state.run_actions(input.actions)?;

    logctx.cleanup_successful();
}

fn generated_config_to_reconfigure_msg(
    member_universe: &[PlatformId],
    coordinator_id: Option<&PlatformId>,
    epoch: Epoch,
    rack_id: RackUuid,
    config: GeneratedConfiguration,
) -> ReconfigureMsg {
    let mut members: BTreeSet<PlatformId> = config
        .members
        .iter()
        .map(|index| member_universe[*index].clone())
        .collect();

    if let Some(coordinator_id) = coordinator_id {
        members.insert(coordinator_id.clone());
    }

    println!("members = {members:#?}");

    let threshold =
        Threshold(usize::max(2, config.threshold.index(members.len())) as u8);

    ReconfigureMsg {
        rack_id,
        epoch,
        last_committed_epoch: None,
        members,
        threshold,
        retry_timeout: Duration::from_millis(RETRY_TIMEOUT_MS),
    }
}
