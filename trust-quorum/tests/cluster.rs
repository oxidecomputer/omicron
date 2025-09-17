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
use secrecy::ExposeSecret;
use slog::{Logger, info, o};
use std::collections::BTreeSet;
use test_strategy::{Arbitrary, proptest};
use trust_quorum::{
    CoordinatorOperation, Epoch, NodeCallerCtx, NodeCommonCtx, PlatformId,
    Threshold,
};
use trust_quorum_test_utils::TqState;
use trust_quorum_test_utils::{
    Event, EventLog,
    nexus::{NexusConfig, NexusOp, NexusReply},
};
use uuid::Uuid;

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

    fn initial_config_events(
        &self,
        config: GeneratedConfiguration,
        down_nodes: BTreeSet<usize>,
    ) -> Vec<Event> {
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
        let crashed_nodes: BTreeSet<_> = down_nodes
            .into_iter()
            .map(|index| member_universe[index].clone())
            .collect();
        let should_abort = crashed_nodes.contains(&coordinator);
        let config = NexusConfig::new(
            epoch,
            last_committed_epoch,
            coordinator,
            members,
            threshold,
        );
        let mut events = vec![Event::InitialSetup {
            member_universe_size: MEMBER_UNIVERSE_SIZE,
            config,
            crashed_nodes,
        }];

        if should_abort {
            events.push(Event::AbortConfiguration(epoch));
        }
        events
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
            let skip_actions = events.is_empty();
            for event in events {
                let affected_nodes = event.affected_nodes();
                self.tq_state.apply_event(event);
                self.check_invariants(affected_nodes)?;
            }
            if skip_actions {
                self.skipped_actions += 1;
            }
        }
        Ok(())
    }

    fn action_to_events(&self, action: Action) -> Vec<Event> {
        match action {
            Action::DeliverEnvelope(selector) => {
                self.action_to_events_deliver_envelope(selector)
            }
            Action::LoadLatestRackSecret(selector) => {
                self.action_to_events_load_latest_rack_secret(selector)
            }
            Action::LoadRackSecret { config, id } => {
                self.action_to_events_load_rack_secret(config, id)
            }
            Action::ClearSecrets(selector) => {
                self.action_to_events_clear_secrets(selector)
            }
            Action::PollPrepareAcks => {
                self.action_to_events_poll_prepare_acks()
            }
            Action::Commit(index) => self.action_to_events_commit(index),
            Action::DeliverNexusReply => {
                self.action_to_events_deliver_nexus_reply()
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
            Action::CrashNode(index) => self.action_to_events_crash_node(index),
            Action::RestartNode { id, connection_order } => {
                self.action_to_events_restart_node(id, connection_order)
            }
        }
    }

    fn action_to_events_deliver_envelope(
        &self,
        selector: Selector,
    ) -> Vec<Event> {
        let mut events = vec![];
        if self.tq_state.bootstrap_network.is_empty() {
            // nothing to do
            return events;
        }
        let destination =
            selector.select(self.tq_state.bootstrap_network.keys());

        // Envelopes should never be sent on the bootstrap network to crashed nodes
        // when events are applied in `TqState::apply_event`.
        //
        // The rationale is that we don't mutate state here, and so we can't
        // choose to pop the message that shouldn't be delivered off the
        // bootstrap network. We could choose not to actually deliver it when
        // applying the event, but that means we have events that don't actually
        // do anything in our event log, which is quite misleading.
        assert!(!self.tq_state.crashed_nodes.contains(destination));

        // We pop from the back and push on the front
        let envelope = self
            .tq_state
            .bootstrap_network
            .get(destination)
            .expect("destination exists")
            .last()
            .expect("envelope exists")
            .clone();
        events.push(Event::DeliverEnvelope(envelope));
        events
    }

    fn action_to_events_crash_node(&self, selector: Selector) -> Vec<Event> {
        let mut faultable = self
            .tq_state
            .member_universe
            .iter()
            .filter(|m| !self.tq_state.crashed_nodes.contains(&m))
            .peekable();

        if faultable.peek().is_none() {
            // All nodes are down
            return vec![];
        }

        let id = selector.select(faultable).clone();
        let latest_config = self.tq_state.nexus.latest_config();
        if id == latest_config.coordinator
            && latest_config.op == NexusOp::Preparing
        {
            // The `AbortConfiguration` simulates Nexus polling and timing
            // out or receiving an error response on node restart because the
            // configuration was lost.
            vec![
                Event::CrashNode(id),
                Event::AbortConfiguration(latest_config.epoch),
            ]
        } else {
            vec![Event::CrashNode(id)]
        }
    }

    fn action_to_events_restart_node(
        &self,
        id: Selector,
        connection_order_indexes: Vec<Index>,
    ) -> Vec<Event> {
        if self.tq_state.crashed_nodes.is_empty() {
            return vec![];
        }

        // Choose the node to restart
        let id = id.select(self.tq_state.crashed_nodes.iter()).clone();

        // Now order the peer connections

        // First find all the peers we want to connect to.
        let mut to_connect: Vec<_> = self
            .tq_state
            .member_universe
            .iter()
            .filter(|id| !self.tq_state.crashed_nodes.contains(id))
            .cloned()
            .collect();

        let total_connections = to_connect.len();

        // Then remove them from `to_connect` and put them into `connection_order`.
        let mut connection_order = vec![];
        for index in connection_order_indexes {
            if to_connect.is_empty() {
                break;
            }
            let i = index.index(to_connect.len());
            let dst = to_connect.swap_remove(i);
            connection_order.push(dst);
        }

        // If there is anything left in `to_connect`, then just extend
        // `connection_order` with it.
        connection_order.extend_from_slice(&to_connect);

        // Ensure we have exactly the number of connections we want
        assert_eq!(connection_order.len(), total_connections);

        vec![Event::RestartNode { id, connection_order }]
    }

    fn action_to_events_load_latest_rack_secret(
        &self,
        selector: Selector,
    ) -> Vec<Event> {
        let mut events = vec![];
        if let Some(c) = self.tq_state.nexus.last_committed_config() {
            let mut loadable = c
                .members
                .iter()
                .filter(|m| !self.tq_state.crashed_nodes.contains(m))
                .peekable();
            if loadable.peek().is_some() {
                let id = selector.select(loadable).clone();
                events.push(Event::LoadRackSecret(id, c.epoch));
            }
        }
        events
    }

    fn action_to_events_clear_secrets(&self, selector: Selector) -> Vec<Event> {
        let mut events = vec![];
        if let Some(c) = self.tq_state.nexus.last_committed_config() {
            let id = selector.select(c.members.iter()).clone();
            events.push(Event::ClearSecrets(id.clone()));
        }
        events
    }

    fn action_to_events_load_rack_secret(
        &self,
        config: Selector,
        id: Selector,
    ) -> Vec<Event> {
        let mut committed_configs_iter = self
            .tq_state
            .nexus
            .configs
            .iter()
            .filter(|c| c.op == NexusOp::Committed)
            .peekable();
        if committed_configs_iter.peek().is_none() {
            // No committed configurations
            return vec![];
        }
        let c = config.select(committed_configs_iter);
        let mut loadable = c
            .members
            .iter()
            .filter(|m| !self.tq_state.crashed_nodes.contains(m))
            .peekable();
        if loadable.peek().is_none() {
            return vec![];
        }
        let id = id.select(c.members.iter()).clone();
        vec![Event::LoadRackSecret(id, c.epoch)]
    }

    fn action_to_events_poll_prepare_acks(&self) -> Vec<Event> {
        let mut events = vec![];
        let latest_config = self.tq_state.nexus.latest_config();
        if latest_config.op != NexusOp::Preparing {
            // No point in checking. Commit or abort has occurred.
            return events;
        }

        // If the coordinator is currently down then Nexus should abort.
        if self.tq_state.crashed_nodes.contains(&latest_config.coordinator) {
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
        let cs = coordinator.get_coordinator_state().unwrap_or_else(|| {
            panic!("coordinator is coordinating: {}", ctx.platform_id())
        });

        // Put the reply on the network
        events.push(Event::SendNexusReplyOnUnderlay(
            NexusReply::AckedPreparesFromCoordinator {
                epoch: coordinator_epoch,
                acks: cs.op().acked_prepares(),
            },
        ));
        events
    }

    fn action_to_events_commit(&self, index: Index) -> Vec<Event> {
        let mut events = vec![];
        let latest_config = self.tq_state.nexus.latest_config();
        if latest_config.op != NexusOp::Committed {
            return events;
        }
        let committable: Vec<_> = latest_config
            .prepared_members
            .difference(&latest_config.committed_members)
            .filter(|m| !self.tq_state.crashed_nodes.contains(m))
            .collect();

        if committable.is_empty() {
            return events;
        }

        let id = index.get(&committable);
        events.push(Event::CommitConfiguration((*id).clone()));
        events
    }

    fn action_to_events_deliver_nexus_reply(&self) -> Vec<Event> {
        if let Some(reply) = self.tq_state.underlay_network.last() {
            vec![Event::DeliverNexusReply(reply.clone())]
        } else {
            vec![]
        }
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
            coordinator.clone(),
            new_members,
            threshold,
        );
        let mut events = vec![Event::Reconfigure(nexus_config)];

        if self.tq_state.crashed_nodes.contains(&coordinator) {
            // This simulates a timeout on the reply from the coordinator which
            // triggers an abort.
            events.push(Event::AbortConfiguration(epoch));
        }
        events
    }

    // At the end of the test, we look at the last configuration. If Nexus
    // is still preparing or has committed the last configuration then we
    // attempt to guarantee that all nodes in the configuration commit and that
    // we can load the identical rack secrets at all of them. If the latest
    // configuration has aborted, we will start a new reconfiguration and drive
    // it to completion.
    fn ensure_liveness(
        &mut self,
        event_log: &mut EventLog,
    ) -> Result<(), TestCaseError> {
        // We need to find all crashed nodes in the current config and the last
        // committed config. We require the latter so that we can gather shares
        // from them to recompute the old rack secret if the coordinator is not
        // yet sending prepare messages.
        let latest_config = self.tq_state.nexus.latest_config().clone();
        let mut crashed: BTreeSet<_> = latest_config
            .members
            .iter()
            .filter(|&id| self.tq_state.crashed_nodes.contains(id))
            .cloned()
            .collect();
        if let Some(last_committed_config) =
            self.tq_state.nexus.last_committed_config()
        {
            crashed.extend(
                last_committed_config
                    .members
                    .iter()
                    .filter(|&id| self.tq_state.crashed_nodes.contains(id))
                    .cloned(),
            );
        }

        // Restart all crashed nodes
        for id in crashed {
            // We aren't trying to randomize the connection order to
            // test safety due to interleavings here. We just are trying
            // to drive the system to an equilibrium state.
            let to_connect: Vec<_> = self
                .tq_state
                .member_universe
                .iter()
                .filter(|id| !self.tq_state.crashed_nodes.contains(id))
                .cloned()
                .collect();
            let event = Event::RestartNode { id, connection_order: to_connect };
            self.record_and_apply_event(event, event_log)?;
        }

        // Deliver all envelopes related to node restart
        self.deliver_all_envelopes(event_log)?;

        match latest_config.op {
            NexusOp::Committed => {
                // Now find all unprepared members and call
                // `Node::prepare_and_commit`
                let unprepared: BTreeSet<_> = latest_config
                    .members
                    .iter()
                    .filter(|&id| !latest_config.prepared_members.contains(id))
                    .cloned()
                    .collect();

                for id in unprepared.clone() {
                    let event = Event::PrepareAndCommit(id);
                    self.record_and_apply_event(event, event_log)?;
                }

                // Deliver all envelopes as a result of calling `PrepareAndCommit`
                self.deliver_all_envelopes(event_log)?;

                // Now find all uncommitted members  that are not also
                // in unprepared (we don't actually inform nexus of the
                // `CommitAck`s here after we successfully `PrepareAndCommit`)
                // and call `Node::commit_configuration`.
                //
                // Note that we could just call `PrepareAndCommit` for all
                // uncommmitted nodes, but this is not what nexus will be doing, and
                // so we do the same thing as the real code here.
                let uncommitted: BTreeSet<_> = latest_config
                    .members
                    .iter()
                    .filter(|&id| {
                        !latest_config.committed_members.contains(id)
                            && !unprepared.contains(id)
                    })
                    .cloned()
                    .collect();

                for id in uncommitted.clone() {
                    let event = Event::CommitConfiguration(id);
                    self.record_and_apply_event(event, event_log)?;
                }

                // Deliver all envelopes as a result of calling `Commit`
                self.deliver_all_envelopes(event_log)?;

                // Ensure all nodes are committed
                for id in &latest_config.members {
                    let (node, ctx) = self
                        .tq_state
                        .sut
                        .nodes
                        .get_mut(id)
                        .expect("node exists");

                    assert!(
                        ctx.persistent_state()
                            .commits
                            .contains(&latest_config.epoch)
                    );

                    // None of the nodes should be coordinating
                    assert!(node.get_coordinator_state().is_none());
                }

                // Trigger loading of rack secrets
                for id in &latest_config.members {
                    let event =
                        Event::LoadRackSecret(id.clone(), latest_config.epoch);
                    self.record_and_apply_event(event, event_log)?;
                }
                // Deliver all envelopes as a result of loading rack secrets
                self.deliver_all_envelopes(event_log)?;

                // At this point all the rack secrets should be available.
                self.compare_all_loaded_rack_secrets(&latest_config);
            }
            NexusOp::Preparing => {
                // After a restart, all nodes should be prepared
                for id in &latest_config.members {
                    let (_, ctx) = self
                        .tq_state
                        .sut
                        .nodes
                        .get_mut(id)
                        .expect("node exists");

                    assert!(
                        ctx.persistent_state()
                            .has_prepared(latest_config.epoch)
                    );
                }

                self.commit_and_load_rack_secrets(event_log, &latest_config)?;

                // At this point all the rack secrets should be available.
                self.compare_all_loaded_rack_secrets(&latest_config);
            }
            NexusOp::Aborted => {
                // Take the aborted configuration, bump the epoch, and try again.
                let mut new_config = latest_config.clone();
                new_config.epoch = latest_config.epoch.next();
                new_config.op = NexusOp::Preparing;
                let event = Event::Reconfigure(new_config.clone());
                self.record_and_apply_event(event, event_log)?;

                // Deliver all envelopes related to `Event::Reconfigure`
                self.deliver_all_envelopes(event_log)?;

                // All nodes should be prepared.
                for id in &new_config.members {
                    let (_, ctx) = self
                        .tq_state
                        .sut
                        .nodes
                        .get_mut(id)
                        .expect("node exists");

                    assert!(
                        ctx.persistent_state().has_prepared(new_config.epoch)
                    );
                }

                self.commit_and_load_rack_secrets(event_log, &new_config)?;

                // At this point all the rack secrets should be available.
                self.compare_all_loaded_rack_secrets(&new_config);
            }
        }

        // We should have no envelopes outgoing on any node as they should be
        // put on the bootstrap network immediately after event application.
        for (_, ctx) in self.tq_state.sut.nodes.values() {
            assert!(ctx.envelopes().next().is_none());
        }

        Ok(())
    }

    // Assume all nodes in the latest configuration are prepared and then commit
    // them and load their rack secrets.
    fn commit_and_load_rack_secrets(
        &mut self,
        event_log: &mut EventLog,
        latest_config: &NexusConfig,
    ) -> Result<(), TestCaseError> {
        // Commit all members
        for id in &latest_config.members {
            let event = Event::CommitConfiguration(id.clone());
            self.record_and_apply_event(event, event_log)?;
        }

        // Deliver all envelopes as a result of calling `Commit`
        self.deliver_all_envelopes(event_log)?;

        // Ensure all nodes are committed and start loading of rack secrets
        for id in &latest_config.members {
            let (node, ctx) =
                self.tq_state.sut.nodes.get_mut(id).expect("node exists");

            assert!(
                ctx.persistent_state().commits.contains(&latest_config.epoch)
            );

            // None of the nodes should be coordinating
            assert!(node.get_coordinator_state().is_none());

            let event = Event::LoadRackSecret(id.clone(), latest_config.epoch);
            self.record_and_apply_event(event, event_log)?;
        }

        // Deliver all envelopes to complete rack secret loading
        self.deliver_all_envelopes(event_log)?;

        Ok(())
    }

    // Load all rack secrets at each node and compare them to each other.
    //
    // The secrets must already be loaded. This method just retreives them.
    fn compare_all_loaded_rack_secrets(&mut self, latest_config: &NexusConfig) {
        let mut members = latest_config.members.iter();
        let id = members.next().unwrap();
        let (node, ctx) =
            self.tq_state.sut.nodes.get_mut(id).expect("node exists");
        let Ok(Some(rs)) = node.load_rack_secret(ctx, latest_config.epoch)
        else {
            panic!(
                "rack secret not loaded for {} at epoch {}",
                ctx.platform_id(),
                latest_config.epoch
            );
        };
        assert!(
            !node.is_collecting_shares_for_rack_secret(latest_config.epoch)
        );

        for id in &latest_config.members {
            let (node, ctx) =
                self.tq_state.sut.nodes.get_mut(id).expect("node exists");
            let Ok(Some(rs2)) = node.load_rack_secret(ctx, latest_config.epoch)
            else {
                panic!(
                    "rack secret not loaded for {} at epoch {}",
                    ctx.platform_id(),
                    latest_config.epoch
                );
            };
            assert_eq!(rs.expose_secret(), rs2.expose_secret());
            assert!(
                !node.is_collecting_shares_for_rack_secret(latest_config.epoch)
            );
        }
    }

    /// Create and apply `DeliverEnvelope` events until all messages get delivered.
    ///
    /// Each delivered envelope can result in new envelopes on the bootstrap
    /// network and so we loop until the system comes to equilibrium.
    fn deliver_all_envelopes(
        &mut self,
        event_log: &mut EventLog,
    ) -> Result<(), TestCaseError> {
        loop {
            let Some((id, envelopes)) =
                self.tq_state.bootstrap_network.first_key_value()
            else {
                return Ok(());
            };

            let envelope = envelopes
                .last()
                .unwrap_or_else(|| panic!("envelope exists at {id}"))
                .clone();
            let event = Event::DeliverEnvelope(envelope);
            self.record_and_apply_event(event, event_log)?;
        }
    }

    fn record_and_apply_event(
        &mut self,
        event: Event,
        event_log: &mut EventLog,
    ) -> Result<(), TestCaseError> {
        event_log.record(&event);
        let affected_nodes = event.affected_nodes();
        self.tq_state.apply_event(event);
        self.check_invariants(affected_nodes)
    }

    /// At every point during the running of the test, invariants over the system
    /// must hold.
    ///
    /// We typically only check the current configuration as the checks hold
    /// inductively as configurations advance.
    ///
    /// Furthermore, we only test the modified node where we can to prevent
    /// having to loop over all nodes when they haven't changed.
    fn check_invariants(
        &self,
        affected_nodes: Vec<PlatformId>,
    ) -> Result<(), TestCaseError> {
        self.invariant_all_nodes_have_same_configuration_per_epoch(
            &affected_nodes,
        )?;
        self.invariant_nodes_have_prepared_if_coordinator_has_acks(
            &affected_nodes,
        )?;
        self.invariant_nodes_have_committed_if_nexus_has_acks(&affected_nodes)?;
        self.invariant_nodes_not_coordinating_and_computing_key_share_simultaneously(&affected_nodes)?;
        self.invariant_no_alarms(&affected_nodes)?;
        self.invariant_expunged_nodes_have_actually_been_expunged(
            &affected_nodes,
        )?;
        Ok(())
    }

    /// For all expunged nodes ensure that either:
    ///  * they know they are expunged
    ///  * have a latest committed configuration where they are still a member
    ///  * have no committed configurations
    fn invariant_expunged_nodes_have_actually_been_expunged(
        &self,
        affected_nodes: &[PlatformId],
    ) -> Result<(), TestCaseError> {
        for id in affected_nodes {
            if self.tq_state.expunged.contains(id) {
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
                    prop_assert!(
                        config.members.contains_key(ctx.platform_id())
                    );
                    prop_assert!(
                        nexus_config.members.contains(ctx.platform_id())
                    );
                }
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
        affected_nodes: &[PlatformId],
    ) -> Result<(), TestCaseError> {
        for id in affected_nodes {
            let (_, ctx) =
                self.tq_state.sut.nodes.get(id).expect("node exists");
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
        affected_nodes: &[PlatformId],
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

        // If any affected node was one of the acked nodes, then it should have
        // prepared for this epoch.
        for id in affected_nodes {
            if acked.contains(id) {
                let (_, ctx) =
                    self.tq_state.sut.nodes.get(&id).expect("node exists");
                prop_assert!(ctx.persistent_state().has_prepared(epoch));
            }
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
        affected_nodes: &[PlatformId],
    ) -> Result<(), TestCaseError> {
        let latest_config = self.tq_state.nexus.latest_config();
        if latest_config.op != NexusOp::Committed {
            return Ok(());
        }

        for id in affected_nodes {
            if latest_config.committed_members.contains(id) {
                let (_, ctx) =
                    self.tq_state.sut.nodes.get(id).expect("node exists");
                let ps = ctx.persistent_state();
                prop_assert!(ps.commits.contains(&latest_config.epoch));
                prop_assert!(ps.has_prepared(latest_config.epoch));
            }
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
        affected_nodes: &[PlatformId],
    ) -> Result<(), TestCaseError> {
        for id in affected_nodes {
            let (node, _) =
                self.tq_state.sut.nodes.get(id).expect("node exists");
            prop_assert!(
                !(node.get_coordinator_state().is_some()
                    && node.is_computing_key_share()),
                "Coordinating and computing key share on node {}",
                id
            );
        }

        Ok(())
    }

    // Ensure there has been no alarm at any affected node
    fn invariant_no_alarms(
        &self,
        affected_nodes: &[PlatformId],
    ) -> Result<(), TestCaseError> {
        for id in affected_nodes {
            let (_, ctx) =
                self.tq_state.sut.nodes.get(id).expect("node exists");
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
    /// Deliver an in-flight bootstrap network msg if there is one.
    ///
    /// The selector here is used to index into the `PlatformIds` of
    /// `test_state.bootstrap_network`.
    #[weight(30)]
    DeliverEnvelope(Selector),

    /// Start loading a rack secret for the latest committed configuration at
    /// the replica determined by the selector into the members of that latest
    /// configuration.
    ///
    /// Latest configuration is determined by looking at `NexusState`.
    #[weight(5)]
    LoadLatestRackSecret(Selector),

    /// Start loading a rack secret for an arbitrary committed configuration at
    /// a node that is a member of that configuration.
    #[weight(5)]
    LoadRackSecret { config: Selector, id: Selector },

    /// Clear all rack secrets at the selected node in the latest committed
    /// configuration if there is one.
    #[weight(2)]
    ClearSecrets(Selector),

    /// Have Nexus poll the coordinator for the latest configuration if it is
    /// still being prepared.
    ///
    /// This updates `NexusConfig::prepared_members` after polling, which
    /// simulates recording this information in CRDB. If Nexus has witnessed
    /// that enough nodes have acked prepares then it changes the config
    /// operation to committed.
    #[weight(10)]
    PollPrepareAcks,

    /// If the current configuration at nexus is marked `NexusOp::Committed`
    /// then call `Node::commit_configuration` for the indexed
    /// node in `NexusConfig::prepared_members` that is not also in
    /// `NexusConfig::committed_members`.
    #[weight(10)]
    Commit(Index),

    /// Deliver in-flight messages to Nexus from the underlay network
    #[weight(10)]
    DeliverNexusReply,

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

    /// Crash a random node in the universe
    #[weight(2)]
    CrashNode(Selector),

    /// Restart a crashed node if there is one
    ///
    /// We randomize the connection order, because that influences the order
    /// that messages sent on reconnect will get delivered to the newly
    /// connected node.
    #[weight(2)]
    RestartNode {
        id: Selector,
        #[any(size_range(MEMBER_UNIVERSE_SIZE-1..MEMBER_UNIVERSE_SIZE).lift())]
        connection_order: Vec<Index>,
    },
}

const MIN_CLUSTER_SIZE: usize = 3;
const MAX_CLUSTER_SIZE: usize = 12;
const MEMBER_UNIVERSE_SIZE: usize = 16;
const MAX_INITIAL_DOWN_NODES: usize = 5;
const MAX_ADDED_NODES: usize = 5;
const MAX_REMOVED_NODES: usize = 3;

// This is how long each test case is
const MIN_ACTIONS: usize = 300;
const MAX_ACTIONS: usize = 2000;

// This is the number of test cases to run. We prefer to run longer tests with
// fewer test cases to maximize the results of each one having an interesting
// interleaving while stepping through multiple configurations. This is
// particularly true since we generate actions before tests run, and therefore
// don't know which actions are valid.
//
// We can always set this much higher for periodic runs, but we want to limit it
// primarily for CI and general testing.
const MAX_TEST_CASES: u32 = 100;

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

// No need to shrink with tqdb available. It's very unlikely to be successful
// anyway.
#[proptest(cases = MAX_TEST_CASES, max_shrink_iters = 0)]
fn test_trust_quorum_protocol(input: TestInput) {
    // We add a uuid so that we can match log files and event traces
    // across multiple proptest runs.
    let test_name = format!("test_trust_quorum_protocol_{}", Uuid::new_v4());
    let logctx = test_setup_log(test_name.as_str());
    let (parent_dir, _) = log_prefix_for_test(logctx.test_name());
    let event_log_path = parent_dir.join(format!("{test_name}.events.json"));
    let mut event_log = EventLog::new(&event_log_path);
    println!("Event log path: {event_log_path}");

    let log = logctx.log.new(o!("component" => "tq-proptest"));
    let mut state = TestState::new(log.clone());

    // Perform the initial setup
    let events = state
        .initial_config_events(input.initial_config, input.initial_down_nodes);
    for event in events {
        event_log.record(&event);
        state.tq_state.apply_event(event);
    }

    // Start executing the actions
    state.run_actions(input.actions, &mut event_log)?;

    // Ensure all nodes in the latest configuration can load the same rack
    // secrets once they are up and there are no more messages to deliver.
    state.ensure_liveness(&mut event_log)?;

    info!(
        log,
        "Test complete";
        "skipped_actions" => state.skipped_actions
    );

    let _ = std::fs::remove_file(event_log_path);
    logctx.cleanup_successful();
}
