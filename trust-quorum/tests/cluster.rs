// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Property based test driving multiple trust quorum nodes

use daft::Diffable;
use dropshot::test_util::log_prefix_for_test;
use omicron_test_utils::dev::test_setup_log;
use prop::sample::Index;
use proptest::collection::{btree_set, size_range};
use proptest::prelude::*;
use proptest::sample::Selector;
use slog::{Logger, info, o};
use std::collections::{BTreeMap, BTreeSet};
use test_strategy::{Arbitrary, proptest};
use trust_quorum::{
    CoordinatorOperation, Epoch, NodeCommonCtx, PlatformId, Threshold,
};
use trust_quorum_test_utils::TqState;
use trust_quorum_test_utils::{
    Event, EventLog,
    nexus::{NexusConfig, NexusOp, NexusReply},
};

/// The state of our test
#[derive(Clone, Diffable)]
struct TestState {
    pub tq_state: TqState,

    /// Keep track of the number of generated `Action`s that get skipped
    ///
    /// Because we generate actions up front, we don't know if they are valid or
    /// not to run during the test. This results in quite a few discards, and we
    /// track them for help in refining the test.
    pub skipped_actions: usize,
}

impl TestState {
    pub fn new(log: Logger) -> TestState {
        TestState { tq_state: TqState::new(log), skipped_actions: 0 }
    }

    fn initial_config_event(
        &self,
        config: GeneratedConfiguration,
        down_nodes: BTreeSet<usize>,
    ) -> Event {
        // `tq_state` doesn't create the member universe until the first event is
        // applied. We duplicate it here so we can create that initial config
        // event.
        let member_universe =
            trust_quorum_test_utils::member_universe(MEMBER_UNIVERSE_SIZE);
        let members: BTreeSet<PlatformId> = config
            .members
            .iter()
            .map(|index| member_universe[*index].clone())
            .collect();
        let threshold = Threshold(usize::max(
            2,
            config.threshold.index(members.len()),
        ) as u8);
        let epoch = Epoch(1);
        let coordinator =
            members.first().cloned().expect("at least one member");
        let last_committed_epoch = None;
        let config = NexusConfig::new(
            epoch,
            last_committed_epoch,
            coordinator,
            members,
            threshold,
        );
        let crashed_nodes = down_nodes
            .into_iter()
            .map(|index| member_universe[index].clone())
            .collect();
        Event::InitialSetup {
            member_universe_size: MEMBER_UNIVERSE_SIZE,
            config,
            crashed_nodes,
        }
    }

    // Execute the proptest generated actions
    pub fn run_actions(
        &mut self,
        actions: Vec<Action>,
        event_log: &mut EventLog,
    ) -> Result<(), TestCaseError> {
        for action in actions {
            let events = self.action_to_events(action);
            for event in &events {
                event_log.record(event);
            }
            let check_invariants = !events.is_empty();
            for event in events {
                self.tq_state.apply_event(event);
            }
            if check_invariants {
                self.check_invariants()?;
            } else {
                self.skipped_actions += 1;
            }
        }
        Ok(())
    }

    fn action_to_events(&self, action: Action) -> Vec<Event> {
        match action {
            Action::DeliverEnvelopes(indices) => {
                self.action_to_events_deliver_envelopes(indices)
            }
            Action::PollPrepareAcks => {
                self.action_to_events_poll_prepare_acks()
            }
            Action::Commit(indices) => self.action_to_events_commit(indices),
            Action::DeliverNexusReplies(n) => {
                self.action_to_events_deliver_nexus_replies(n)
            }
            Action::Reconfigure {
                num_added_nodes,
                removed_nodes,
                threshold,
                coordinator,
            } => self.action_to_events_reconfigure(
                num_added_nodes,
                removed_nodes,
                threshold,
                coordinator,
            ),
        }
    }

    fn action_to_events_deliver_envelopes(
        &self,
        indices: Vec<Index>,
    ) -> Vec<Event> {
        let mut events = vec![];
        let destinations: Vec<_> =
            self.tq_state.bootstrap_network.keys().cloned().collect();
        if destinations.is_empty() {
            // nothing to do
            return events;
        }

        // Add an event only if there is actually an envelope to send
        let mut counts = BTreeMap::new();
        for index in indices {
            let id = index.get(&destinations);
            let count = counts.entry(id).or_insert(0usize);
            *count += 1;
            let num_envelopes = self
                .tq_state
                .bootstrap_network
                .get(id)
                .expect("destination exists")
                .len();
            if *count <= num_envelopes {
                events.push(Event::DeliverEnvelope { destination: id.clone() });
            }
        }

        events
    }

    fn action_to_events_poll_prepare_acks(&self) -> Vec<Event> {
        let mut events = vec![];
        let latest_config = self.tq_state.nexus.latest_config();
        if latest_config.op != NexusOp::Preparing {
            // No point in checking. Commit or abort has occurred.
            return events;
        }

        // If the coordinator has crashed then Nexus should abort.
        // Crashing is not actually implemented yet, but it will be.
        if self
            .tq_state
            .faults
            .crashed_nodes
            .contains(&latest_config.coordinator)
        {
            events.push(Event::AbortConfiguration(latest_config.epoch));
            return events;
        }

        // Lookup the coordinator node
        let (coordinator, ctx) = self
            .tq_state
            .sut
            .nodes
            .get(&latest_config.coordinator)
            .expect("coordinator exists");

        // It's possible the coordinator hasn't yet received the reconfiguration
        // from nexus. Let's ensure it has.
        let coordinator_epoch = ctx
            .persistent_state()
            .latest_config()
            .map_or(Epoch(0), |c| c.epoch);
        if coordinator_epoch != latest_config.epoch {
            return events;
        }

        // Poll the coordinator for acks.
        //
        // We expect the coordinator to still be coordinating since it has not
        // crashed and nexus is still preparing.
        //
        // In a real system this request would go over the network, but would
        // end up at the same place.
        let cs = coordinator
            .get_coordinator_state()
            .expect("coordinator is coordinating");

        // Put the reply on the network
        events.push(Event::SendNexusReplyOnUnderlay(
            NexusReply::AckedPreparesFromCoordinator {
                epoch: coordinator_epoch,
                acks: cs.op().acked_prepares(),
            },
        ));
        events
    }

    fn action_to_events_commit(&self, indices: Vec<Index>) -> Vec<Event> {
        let mut events = vec![];
        let latest_config = self.tq_state.nexus.latest_config();
        if latest_config.op != NexusOp::Committed {
            return events;
        }
        let committable: Vec<_> = latest_config
            .prepared_members
            .difference(&latest_config.committed_members)
            .collect();

        if committable.is_empty() {
            return events;
        }

        // De-duplicate the Index->PlatformId mapping
        let mut nodes: BTreeSet<PlatformId> = BTreeSet::new();
        for index in indices {
            let id = *index.get(&committable);
            nodes.insert(id.clone());
        }
        for node in nodes {
            events.push(Event::CommitConfiguration(node));
        }
        events
    }

    fn action_to_events_deliver_nexus_replies(&self, n: usize) -> Vec<Event> {
        let mut events = vec![];
        let n = usize::min(n, self.tq_state.underlay_network.len());
        for _ in 0..n {
            events.push(Event::DeliverNexusReply);
        }
        events
    }

    fn action_to_events_reconfigure(
        &self,
        num_added_nodes: usize,
        removed_nodes: Vec<Selector>,
        threshold: Index,
        coordinator: Selector,
    ) -> Vec<Event> {
        let latest_epoch = self.tq_state.nexus.latest_config().epoch;
        let last_committed_config = self.tq_state.nexus.last_committed_config();
        // We must leave at least one node available to coordinate between the
        // new and old configurations.
        let (new_members, coordinator) = match last_committed_config {
            Some(c) => {
                let possible_num_nodes_to_add = usize::min(
                    num_added_nodes,
                    MAX_CLUSTER_SIZE - c.members.len(),
                );

                // How many nodes can we add taking into account expunged nodes
                // and the existing cluster?
                let num_nodes_to_add = usize::min(
                    MEMBER_UNIVERSE_SIZE
                        - c.members.len()
                        - self.tq_state.expunged.len(),
                    possible_num_nodes_to_add,
                );

                // What is the max number of nodes that we can remove such that:
                //  * 1 node is in both the new and old cluster
                //  * The total number of nodes in the new cluster is
                //    >= MIN_CLUSTER_SIZE
                let max_nodes_to_remove = (c.members.len() - 1
                    + num_nodes_to_add)
                    .saturating_sub(MIN_CLUSTER_SIZE);

                // Find a potential coordinator node
                //
                // We can only start a reconfiguration if Nexus has an
                // acknowledgement that at least one node has seen the commit.
                if c.committed_members.is_empty() {
                    return vec![];
                }
                let coordinator =
                    coordinator.select(c.committed_members.iter());

                // First, find the set of nodes to remove
                let mut nodes_to_remove = BTreeSet::new();
                for s in removed_nodes {
                    // The same selection can be chosen more than once. so we
                    // must add the extra check rather than shrinking the length
                    // of the `removed_nodes` iterator with `take`.
                    if nodes_to_remove.len() == max_nodes_to_remove {
                        break;
                    }
                    let node = s.select(c.members.iter());
                    if node != coordinator {
                        nodes_to_remove.insert(node.clone());
                    }
                }

                // Then find the set of nodes to add
                //
                // Just pick the first set of nodes in `member_universe`
                // that are not in the current membership and not expunged.
                let mut nodes_to_add = BTreeSet::new();
                for id in self.tq_state.member_universe.iter() {
                    if nodes_to_add.len() == num_nodes_to_add {
                        break;
                    }
                    if !self.tq_state.expunged.contains(id)
                        && !c.members.contains(id)
                    {
                        nodes_to_add.insert(id.clone());
                    }
                }

                // Finally, create our new membership
                let mut new_members = c.members.clone();
                for id in nodes_to_remove {
                    new_members.remove(&id);
                }
                for id in nodes_to_add {
                    new_members.insert(id);
                }
                (new_members, coordinator.clone())
            }
            None => {
                // We are generating a new config
                if num_added_nodes < MIN_CLUSTER_SIZE {
                    // Nothing to do here.
                    return vec![];
                }
                // Pick the first `num_added_nodes` from member_universe
                // It's as good a choice as any and deterministic
                let new_members: BTreeSet<_> = self
                    .tq_state
                    .member_universe
                    .iter()
                    .take(num_added_nodes)
                    .cloned()
                    .collect();

                // There is no last committed configuration then we can go ahead and
                // choose any node. We just pick the first one for simplicity and determinism.
                let coordinator =
                    coordinator.select(new_members.iter()).clone();
                (new_members, coordinator)
            }
        };
        let threshold =
            Threshold(usize::max(2, threshold.index(new_members.len())) as u8);
        let epoch = latest_epoch.next();

        // Find a coordinator from the last committed configurarion that has
        // acknowledged the commit from Nexus. If there isn't a node that
        // acknowledged the commit, then we can't reconfigure yet.
        //
        let last_committed_epoch = last_committed_config.map(|c| c.epoch);
        let nexus_config = NexusConfig::new(
            epoch,
            last_committed_epoch,
            coordinator,
            new_members,
            threshold,
        );
        vec![Event::Reconfigure(nexus_config)]
    }

    /// At every point during the running of the test, invariants over the system
    /// must hold.
    ///
    /// We typically only check the current configuration as the checks hold
    /// inductively as configurations advance.
    fn check_invariants(&self) -> Result<(), TestCaseError> {
        self.invariant_all_nodes_have_same_configuration_per_epoch()?;
        self.invariant_nodes_have_prepared_if_coordinator_has_acks()?;
        self.invariant_nodes_have_committed_if_nexus_has_acks()?;
        self.invariant_nodes_not_coordinating_and_computing_key_share_simultaneously()?;
        self.invariant_no_alarms()?;
        self.invariant_expunged_nodes_have_actually_been_expunged()?;
        Ok(())
    }

    /// For all expunged nodes ensure that either:
    ///  * they know they are expunged
    ///  * have a latest committed configuration where they are still a member
    ///  * have no committed configurations
    fn invariant_expunged_nodes_have_actually_been_expunged(
        &self,
    ) -> Result<(), TestCaseError> {
        for id in &self.tq_state.expunged {
            let (_, ctx) =
                self.tq_state.sut.nodes.get(id).expect("node exists");
            let ps = ctx.persistent_state();
            if ps.is_expunged() {
                continue;
            }
            if let Some(config) = ps.latest_committed_configuration() {
                let nexus_config = self
                    .tq_state
                    .nexus
                    .configs
                    .get(&config.epoch)
                    .expect("config exists");
                prop_assert!(config.members.contains_key(ctx.platform_id()));
                prop_assert!(nexus_config.members.contains(ctx.platform_id()));
            } else {
                continue;
            }
        }

        Ok(())
    }

    /// If a node has a given configuration it must match all other nodes with
    /// that configuration.
    ///
    /// Sometimes nodes may not have a configuration for a given epoch.
    fn invariant_all_nodes_have_same_configuration_per_epoch(
        &self,
    ) -> Result<(), TestCaseError> {
        for (id, (_, ctx)) in &self.tq_state.sut.nodes {
            let diff = self
                .tq_state
                .all_coordinated_configs
                .diff(&ctx.persistent_state().configs);
            // No new configs exist
            prop_assert!(
                diff.added.is_empty(),
                "unexpected config found for {id}"
            );

            // No configs are different
            prop_assert_eq!(
                diff.modified().count(),
                0,
                "config modified for {}",
                id
            );
        }
        Ok(())
    }

    /// If a node is coordinating by sending `Prepare` msgs, then it should
    /// only have acknowledgments from nodes that have seen the `Prepare`.
    fn invariant_nodes_have_prepared_if_coordinator_has_acks(
        &self,
    ) -> Result<(), TestCaseError> {
        let (acked, epoch) = {
            let latest_config = self.tq_state.nexus.latest_config();
            let (node, _) = self
                .tq_state
                .sut
                .nodes
                .get(&latest_config.coordinator)
                .expect("node exists");

            let (acked, coordinating_epoch) = node
                .get_coordinator_state()
                .map_or((BTreeSet::new(), Epoch(0)), |cs| {
                    if let CoordinatorOperation::Prepare {
                        prepare_acks, ..
                    } = cs.op()
                    {
                        (prepare_acks.clone(), cs.reconfigure_msg().epoch())
                    } else {
                        (BTreeSet::new(), Epoch(0))
                    }
                });
            // Make sure the coordinator actually is coordinating for this epoch
            if coordinating_epoch != latest_config.epoch {
                return Ok(());
            }
            (acked, latest_config.epoch)
        };
        // Make sure the coordinator actually is coordinating for this epoch

        for id in acked {
            let (_, ctx) =
                self.tq_state.sut.nodes.get(&id).expect("node exists");
            prop_assert!(ctx.persistent_state().has_prepared(epoch));
        }

        Ok(())
    }

    /// Ensure that if the latest nexus configuration has been committed
    /// and it has seen commit acks in `NexusConfig::committed_members`
    /// then those members have also persisted a commit for that epoch.
    ///
    /// Committed nodes must also have prepared - meaning they have a
    /// configuration and share for this epoch.
    fn invariant_nodes_have_committed_if_nexus_has_acks(
        &self,
    ) -> Result<(), TestCaseError> {
        let latest_config = self.tq_state.nexus.latest_config();
        if latest_config.op != NexusOp::Committed {
            return Ok(());
        }

        for id in &latest_config.committed_members {
            let (_, ctx) =
                self.tq_state.sut.nodes.get(&id).expect("node exists");
            let ps = ctx.persistent_state();
            prop_assert!(ps.commits.contains(&latest_config.epoch));
            prop_assert!(ps.has_prepared(latest_config.epoch));
        }

        Ok(())
    }

    // A node cannot be coordinating a reconfiguration and computing a key share
    // at the same time. The following two conditions based on local knowledge
    // are mutually exclusive.
    //
    // * If a node is coordinating a reconfiguration then they are creating the
    //   key shares for the newest configuration and have a key share for the
    //   latest committed configuration.
    // * If a node is computing a key share it means that they don't have the a
    //   key share for the latest committed configuration that they know of.
    fn invariant_nodes_not_coordinating_and_computing_key_share_simultaneously(
        &self,
    ) -> Result<(), TestCaseError> {
        for (id, (node, _)) in &self.tq_state.sut.nodes {
            prop_assert!(
                !(node.get_coordinator_state().is_some()
                    && node.is_computing_key_share()),
                "Coordinating and computing key share on node {}",
                id
            );
        }

        Ok(())
    }

    // Ensure there has been no alarm at any node
    fn invariant_no_alarms(&self) -> Result<(), TestCaseError> {
        for (id, (_, ctx)) in &self.tq_state.sut.nodes {
            let alarms = ctx.alarms();
            prop_assert!(
                alarms.is_empty(),
                "Alarms found for {}: {:#?}",
                id,
                alarms
            );
        }
        Ok(())
    }
}

// A high-level set of generated actions to drive the test forward.
#[derive(Debug, Arbitrary)]
#[allow(clippy::large_enum_variant)]
pub enum Action {
    /// For each indexed member deliver an in-flight bootstrap network msg if
    /// there is one.
    ///
    /// The indexes here are used to index into the `PlatformIds` of
    /// `test_state.bootstrap_network`.
    ///
    /// We may deliver more than one message to each member.
    #[weight(4)]
    DeliverEnvelopes(
        #[any(size_range(1..MAX_DELIVERED_ENVELOPES).lift())] Vec<Index>,
    ),

    /// Have Nexus poll the coordinator for the latest configuration if it is
    /// still being prepared.
    ///
    /// This updates `NexusConfig::prepared_members` after polling, which
    /// simulates recording this information in CRDB. If Nexus has witnessed
    /// that enough nodes have acked prepares then it changes the config
    /// operation to committed.
    #[weight(4)]
    PollPrepareAcks,

    /// If the current configuration at nexus is marked `NexusOp::Committed`
    /// then call `Node::commit_configuration` for each indexed
    /// node in `NexusConfig::prepared_members` that is not also in
    /// `NexusConfig::committed_members`.
    #[weight(4)]
    Commit(#[any(size_range(1..MAX_CONCURRENT_COMMITS).lift())] Vec<Index>),

    /// Deliver in-flight messages to Nexus from the underlay network
    #[weight(4)]
    DeliverNexusReplies(#[strategy(1..10usize)] usize),

    /// Generate a new configuration by adding a number of *new* (non-expunged)
    /// nodes to the cluster from `member_universe` and removing the specific
    /// nodes in the current cluster given by the indices `removed_nodes`.
    #[weight(1)]
    Reconfigure {
        #[strategy(0..MAX_ADDED_NODES)]
        num_added_nodes: usize,
        #[any(size_range(0..MAX_REMOVED_NODES).lift())]
        removed_nodes: Vec<Selector>,
        threshold: Index,
        coordinator: Selector,
    },
}

const MIN_CLUSTER_SIZE: usize = 3;
const MAX_CLUSTER_SIZE: usize = 20;
const MEMBER_UNIVERSE_SIZE: usize = 40;
const MAX_INITIAL_DOWN_NODES: usize = 5;
const MAX_ADDED_NODES: usize = 5;
const MAX_REMOVED_NODES: usize = 3;
const MAX_DELIVERED_ENVELOPES: usize = 20;
const MAX_CONCURRENT_COMMITS: usize = 10;
const MIN_ACTIONS: usize = 100;
const MAX_ACTIONS: usize = 1000;

/// Information about configurations used at test generation time
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
    #[strategy(btree_set(0..MEMBER_UNIVERSE_SIZE, MIN_CLUSTER_SIZE..MAX_CLUSTER_SIZE))]
    pub members: BTreeSet<usize>,

    /// An index is roughly equivalent to a threshold, since a threshold cannot
    /// exceed the number of members in a configuration. Using the index is
    /// very efficient as opposed to a `prop_flat_map`. When we calculate the
    /// threshold from the index we use max(2, Index), since the minimum
    /// threshold is always 2.
    pub threshold: Index,
}

#[derive(Debug, Arbitrary)]
pub struct TestInput {
    initial_config: GeneratedConfiguration,

    // We choose a set of nodes to be crashed, resulting in them being
    // disconnected from every other node.
    #[strategy(btree_set(0..MEMBER_UNIVERSE_SIZE, 0..MAX_INITIAL_DOWN_NODES))]
    initial_down_nodes: BTreeSet<usize>,
    #[any(size_range(MIN_ACTIONS..MAX_ACTIONS).lift())]
    actions: Vec<Action>,
}

#[proptest]
fn test_trust_quorum_protocol(input: TestInput) {
    let logctx = test_setup_log("test_trust_quorum_protocol");
    let (parent_dir, prefix) = log_prefix_for_test(logctx.test_name());
    let event_log_path = parent_dir.join(format!("{prefix}-events.json"));
    let mut event_log = EventLog::new(&event_log_path);

    let log = logctx.log.new(o!("component" => "tq-proptest"));
    let mut state = TestState::new(log.clone());

    // Perform the initial setup
    let event = state
        .initial_config_event(input.initial_config, input.initial_down_nodes);
    event_log.record(&event);
    state.tq_state.apply_event(event);

    // Start executing the actions
    state.run_actions(input.actions, &mut event_log)?;

    info!(
        log,
        "Test complete";
        "skipped_actions" => state.skipped_actions
    );

    let _ = std::fs::remove_file(event_log_path);
    logctx.cleanup_successful();
}
