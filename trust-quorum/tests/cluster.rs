// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Property based test driving multiple trust quorum nodes

use daft::Diffable;
use iddqd::id_ord_map::RefMut;
use iddqd::{IdOrdItem, IdOrdMap, id_upcast};
use omicron_test_utils::dev::test_setup_log;
use omicron_uuid_kinds::RackUuid;
use prop::sample::Index;
use proptest::collection::{btree_set, size_range};
use proptest::prelude::*;
use proptest::sample::Selector;
use slog::{Logger, info, o};
use std::collections::{BTreeMap, BTreeSet};
use test_strategy::{Arbitrary, proptest};
use trust_quorum::{
    Configuration, CoordinatorOperation, Envelope, Epoch, Node, NodeCallerCtx,
    NodeCommonCtx, NodeCtx, PeerMsgKind, PlatformId, ReconfigureMsg, Threshold,
};

/// The system under test
///
/// This is our real code.
pub struct Sut {
    /// All nodes in the member universe
    pub nodes: BTreeMap<PlatformId, (Node, NodeCtx)>,
}

impl Sut {
    pub fn new(log: &Logger, universe: Vec<PlatformId>) -> Sut {
        let nodes = universe
            .into_iter()
            .map(|id| {
                let mut ctx = NodeCtx::new(id.clone());
                let node = Node::new(log, &mut ctx);
                (id, (node, ctx))
            })
            .collect();
        Sut { nodes }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum NexusOp {
    Committed,
    Aborted,
    Preparing,
}

/// A single nexus configuration
#[derive(Debug)]
pub struct NexusConfig {
    op: NexusOp,
    epoch: Epoch,
    last_committed_epoch: Option<Epoch>,
    coordinator: PlatformId,
    members: BTreeSet<PlatformId>,
    // This is our `K` parameter
    threshold: Threshold,

    // This is our `Z` parameter.
    //
    // Nexus can commit when it has seen K+Z prepare acknowledgements
    //
    // Only nexus needs to know this value since it alone determines when a
    // commit may occur.
    commit_crash_tolerance: u8,

    prepared_members: BTreeSet<PlatformId>,
    committed_members: BTreeSet<PlatformId>,
}

impl NexusConfig {
    pub fn new(
        epoch: Epoch,
        last_committed_epoch: Option<Epoch>,
        coordinator: PlatformId,
        members: BTreeSet<PlatformId>,
        threshold: Threshold,
    ) -> NexusConfig {
        // We want a few extra nodes beyond `threshold` to ack before we commit.
        // This is the number of nodes that can go offline while still allowing
        // an unlock to occur.
        let commit_crash_tolerance = match members.len() - threshold.0 as usize
        {
            0..=1 => 0,
            2..=4 => 1,
            5..=7 => 2,
            _ => 3,
        };
        NexusConfig {
            op: NexusOp::Preparing,
            epoch,
            last_committed_epoch,
            coordinator,
            members,
            threshold,
            commit_crash_tolerance,
            prepared_members: BTreeSet::new(),
            committed_members: BTreeSet::new(),
        }
    }

    pub fn to_reconfigure_msg(&self, rack_id: RackUuid) -> ReconfigureMsg {
        ReconfigureMsg {
            rack_id,
            epoch: self.epoch,
            last_committed_epoch: self.last_committed_epoch,
            members: self.members.clone(),
            threshold: self.threshold,
        }
    }

    // Are there enough prepared members to commit?
    pub fn can_commit(&self) -> bool {
        self.prepared_members.len()
            >= (self.threshold.0 + self.commit_crash_tolerance) as usize
    }
}

impl IdOrdItem for NexusConfig {
    type Key<'a> = Epoch;

    fn key(&self) -> Self::Key<'_> {
        self.epoch
    }

    id_upcast!();
}

/// A model of Nexus's view of the world during the test
pub struct NexusState {
    // No reason to change the rack_id
    pub rack_id: RackUuid,

    pub configs: IdOrdMap<NexusConfig>,
}

impl NexusState {
    pub fn new() -> NexusState {
        NexusState { rack_id: RackUuid::new_v4(), configs: IdOrdMap::new() }
    }

    // Create a `ReconfigureMsg` for the latest nexus config
    pub fn reconfigure_msg_for_latest_config(
        &self,
    ) -> (&PlatformId, ReconfigureMsg) {
        let config = self.configs.iter().last().expect("at least one config");
        (&config.coordinator, config.to_reconfigure_msg(self.rack_id))
    }

    /// Abort the latest reconfiguration attempt
    pub fn abort_reconfiguration(&mut self) {
        let config = self.configs.iter().last().expect("at least one config");
        // Can only abort while preparing
        assert_eq!(config.op, NexusOp::Preparing);
    }

    pub fn latest_config(&self) -> &NexusConfig {
        self.configs.iter().last().expect("at least one config")
    }

    pub fn latest_config_mut(&mut self) -> RefMut<'_, NexusConfig> {
        self.configs.iter_mut().last().expect("at least one config")
    }

    pub fn last_committed_config(&self) -> Option<&NexusConfig> {
        // IdOrdMap doesn't allow reverse iteration.
        // We therefore iterate through all configs to find the latest committed one.
        // We could track this out of band but that leaves more room for error.
        let mut found: Option<&NexusConfig> = None;
        for c in &self.configs {
            if c.op == NexusOp::Committed {
                found = Some(c)
            }
        }
        found
    }
}

/// Faults in our system. It's useful to keep these self contained and not
/// in separate fields in `TestState` so that we can access them all at once
/// independently of other `TestState` fields.
#[derive(Default)]
pub struct Faults {
    // We allow nodes to crash and restart and therefore track crashed nodes here.
    //
    // A crashed node is implicitly disconnected from every other node. We don't
    // bother storing the pairs in `disconnected_nodes`, but instead check both
    // fields when necessary.
    pub crashed_nodes: BTreeSet<PlatformId>,

    /// The set of disconnected nodes
    pub disconnected_nodes: DisconnectedNodes,
}

impl Faults {
    pub fn is_connected(&self, node1: PlatformId, node2: PlatformId) -> bool {
        !self.crashed_nodes.contains(&node1)
            && !self.crashed_nodes.contains(&node2)
            && !self.disconnected_nodes.contains(node1, node2)
    }
}

/// For cardinality purposes, we assume all nodes are connected and explicitly
/// disconnect some of them. This allows us to track and compare much less data.
#[derive(Default)]
pub struct DisconnectedNodes {
    // We sort each pair on insert for quick lookups
    pairs: BTreeSet<(PlatformId, PlatformId)>,
}

impl DisconnectedNodes {
    // Return true if the pair is newly inserted
    pub fn insert(&mut self, node1: PlatformId, node2: PlatformId) -> bool {
        assert_ne!(node1, node2);

        let pair = if node1 < node2 { (node1, node2) } else { (node2, node1) };
        self.pairs.insert(pair)
    }

    // Return true if the pair of nodes is disconnected, false otherwise
    pub fn contains(&self, node1: PlatformId, node2: PlatformId) -> bool {
        assert_ne!(node1, node2);
        let pair = if node1 < node2 { (node1, node2) } else { (node2, node1) };
        self.pairs.contains(&pair)
    }
}

pub enum NexusReply {
    CommitAck { from: PlatformId, epoch: Epoch },
}

/// The state of our test
struct TestState {
    /// A logger for our test
    pub log: Logger,

    /// Our system under test
    pub sut: Sut,

    /// All in flight messages between nodes
    pub bootstrap_network: BTreeMap<PlatformId, Vec<Envelope>>,

    /// All in flight responses to nexus. We don't model the requests, as those
    /// are `Node` public method calls. But we don't want to synchronously
    /// update nexus state as a result of those calls, because that ruins any
    /// possible interleaving with other actions.
    ///
    /// This is a way to allow interleaving of nexus replies without changing
    /// the Node API to accept a separate set of Nexus messages and return
    /// messages. We may decide that we want to do that, but for now we'll stick
    /// with a concrete `Node` method based API that is "triggered" by nexus
    /// messages.
    pub underlay_network: Vec<NexusReply>,

    /// A model of Nexus's view of the world during the test
    pub nexus: NexusState,

    /// A cache of our member universe, so we only have to generate it once
    pub member_universe: Vec<PlatformId>,

    /// All possible system faults in our test
    pub faults: Faults,

    /// All configurations ever generated by a coordinator.
    ///
    /// If an epoch got skipped due to a crashed coordinator then there will not
    /// be a configuration for that epoch.
    pub all_coordinated_configs: IdOrdMap<Configuration>,

    /// Expunged nodes cannot be added to a cluster. We never reuse nodes in
    /// this test. We include nodes here that may not know yet that they have
    /// been expunged in the `Sut`.
    pub expunged: BTreeSet<PlatformId>,

    /// Keep track of the number of generated `Action`s that get skipped
    ///
    /// Because we generate actions up front, we don't know if they are valid or
    /// not to run during the test. This results in quite a few discards, and we
    /// track them for help in refining the test.
    pub skipped_actions: usize,
}

impl TestState {
    pub fn new(log: Logger) -> TestState {
        let sut = Sut::new(&log, member_universe());
        TestState {
            log: log.new(o!("component" => "tq-proptest")),
            sut,
            bootstrap_network: BTreeMap::new(),
            underlay_network: Vec::new(),
            nexus: NexusState::new(),
            member_universe: member_universe(),
            faults: Faults::default(),
            all_coordinated_configs: IdOrdMap::new(),
            expunged: BTreeSet::new(),
            skipped_actions: 0,
        }
    }

    pub fn create_nexus_initial_config(
        &mut self,
        config: GeneratedConfiguration,
    ) {
        let members: BTreeSet<PlatformId> = config
            .members
            .iter()
            .map(|index| self.member_universe[*index].clone())
            .collect();
        let threshold = Threshold(usize::max(
            2,
            config.threshold.index(members.len()),
        ) as u8);
        let epoch = Epoch(1);
        let coordinator =
            members.first().cloned().expect("at least one member");
        let last_committed_epoch = None;
        let nexus_config = NexusConfig::new(
            epoch,
            last_committed_epoch,
            coordinator,
            members,
            threshold,
        );
        self.nexus.configs.insert_unique(nexus_config).expect("new config");
    }

    pub fn setup_initial_connections(&mut self, down_nodes: BTreeSet<usize>) {
        self.faults.crashed_nodes = down_nodes
            .into_iter()
            .map(|index| self.member_universe[index].clone())
            .collect();

        for (from, (node, ctx)) in self
            .sut
            .nodes
            .iter_mut()
            .filter(|(id, _)| !self.faults.crashed_nodes.contains(id))
        {
            for to in self.member_universe.iter().filter(|id| {
                !self.faults.crashed_nodes.contains(id) && from != *id
            }) {
                node.on_connect(ctx, to.clone());
            }
        }
    }

    /// Send the latest `ReconfigureMsg` from `Nexus` to the coordinator node
    ///
    /// If the node is not available, then abort the configuration at nexus
    pub fn send_reconfigure_msg(&mut self) {
        let (coordinator, msg) = self.nexus.reconfigure_msg_for_latest_config();
        let epoch_to_config = msg.epoch;
        if self.faults.crashed_nodes.contains(coordinator) {
            // We must abort the configuration. This mimics a timeout.
            self.nexus.abort_reconfiguration();
        } else {
            let (node, ctx) = self
                .sut
                .nodes
                .get_mut(coordinator)
                .expect("coordinator exists");

            node.coordinate_reconfiguration(ctx, msg)
                .expect("valid configuration");

            // Do we have a `Configuration` for this epoch yet?
            //
            // For most reconfigurations, shares for the last committed
            // configuration must be retrieved before the configuration is
            // generated and saved in the persistent state.
            let latest_persisted_config =
                ctx.persistent_state().latest_config().expect("config exists");
            if latest_persisted_config.epoch == epoch_to_config {
                // Save the configuration for later
                self.all_coordinated_configs
                    .insert_unique(latest_persisted_config.clone())
                    .expect("unique");
            }
        }
    }

    /// Check postcondition assertions after initial configuration
    pub fn postcondition_initial_configuration(
        &mut self,
    ) -> Result<(), TestCaseError> {
        let (coordinator, msg) = self.nexus.reconfigure_msg_for_latest_config();

        // The coordinator should have received the `ReconfigureMsg` from Nexus
        if !self.faults.crashed_nodes.contains(coordinator) {
            let (node, ctx) = self
                .sut
                .nodes
                .get_mut(coordinator)
                .expect("coordinator exists");
            let mut connected_members = 0;
            // The coordinator should start preparing by sending a `PrepareMsg` to all
            // connected nodes in the membership set.
            for member in
                msg.members.iter().filter(|&id| id != coordinator).cloned()
            {
                if self.faults.is_connected(coordinator.clone(), member.clone())
                {
                    connected_members += 1;
                    let msg_found = ctx.envelopes().any(|envelope| {
                        envelope.to == member
                            && envelope.from == *coordinator
                            && matches!(
                                envelope.msg.kind,
                                PeerMsgKind::Prepare { .. }
                            )
                    });
                    prop_assert!(msg_found);
                }
            }
            assert_eq!(connected_members, ctx.envelopes().count());

            // The coordinator should be in the prepare phase
            let cs = node.get_coordinator_state().expect("is coordinating");
            assert!(matches!(cs.op(), CoordinatorOperation::Prepare { .. }));

            // The persistent state should have changed
            assert!(ctx.persistent_state_change_check_and_reset());
            assert!(ctx.persistent_state().has_prepared(msg.epoch));
            assert!(ctx.persistent_state().latest_committed_epoch().is_none());
        }

        Ok(())
    }

    /// Put any outgoing coordinator messages from the latest configuration on the wire
    pub fn send_envelopes_from_coordinator(&mut self) {
        let coordinator = {
            let (coordinator, _) =
                self.nexus.reconfigure_msg_for_latest_config();
            coordinator.clone()
        };
        self.send_envelopes_from(&coordinator);
    }

    pub fn send_envelopes_from(&mut self, id: &PlatformId) {
        let (_, ctx) = self.sut.nodes.get_mut(id).expect("node exists");
        for envelope in ctx.drain_envelopes() {
            let msgs =
                self.bootstrap_network.entry(envelope.to.clone()).or_default();
            msgs.push(envelope);
        }
    }

    // Execute the proptest generated actions
    pub fn run_actions(
        &mut self,
        actions: Vec<Action>,
    ) -> Result<(), TestCaseError> {
        for action in actions {
            let skipped = match action {
                Action::DeliverEnvelopes(indices) => {
                    self.action_deliver_envelopes(indices)
                }
                Action::PollPrepareAcks => self.action_poll_prepare_acks(),
                Action::Commit(indices) => self.action_commit(indices),
                Action::DeliverNexusReplies(n) => {
                    self.action_deliver_nexus_replies(n)
                }
                Action::Reconfigure {
                    num_added_nodes,
                    removed_nodes,
                    threshold,
                    coordinator,
                } => self.action_reconfigure(
                    num_added_nodes,
                    removed_nodes,
                    threshold,
                    coordinator,
                ),
            };

            if skipped {
                self.skipped_actions += 1;
            } else {
                self.check_invariants()?;
            }
        }
        Ok(())
    }

    // Deliver network messages to generated destinations
    fn action_deliver_envelopes(&mut self, indices: Vec<Index>) -> bool {
        let destinations: Vec<_> =
            self.bootstrap_network.keys().cloned().collect();
        if destinations.is_empty() {
            // nothing to do
            return true;
        }
        for index in indices {
            let id = index.get(&destinations);
            if let Some(envelope) =
                self.bootstrap_network.get_mut(id).unwrap().pop()
            {
                let (node, ctx) =
                    self.sut.nodes.get_mut(id).expect("destination exists");
                node.handle(ctx, envelope.from, envelope.msg);

                // If this is the first time we've seen a configuration, track it
                //
                // We have to do this here because for reconfigurations, shares
                // for the last committed reconfiguration are gathered before
                // the config is created. We don't know exactly when config
                // generation occurs, but know that it happens after envelopes
                // are delivered, except for configurations that don't have
                // a last committed config. This is normally the initial
                // configuration, but can be later ones if the initial config
                // is aborted.
                if ctx.persistent_state_change_check_and_reset() {
                    if let Some(latest_config) =
                        ctx.persistent_state().latest_config()
                    {
                        if !self
                            .all_coordinated_configs
                            .contains_key(&latest_config.epoch)
                        {
                            // The coordinator must be the first node to create
                            // the configuration.
                            assert_eq!(
                                &latest_config.coordinator,
                                ctx.platform_id()
                            );

                            self.all_coordinated_configs
                                .insert_unique(latest_config.clone())
                                .expect("unique config");
                        }
                    }
                }

                // Send any messages as a result of handling this message
                send_envelopes(ctx, &mut self.bootstrap_network);
            }
        }

        // Remove any destinations with zero messages in-flight
        self.bootstrap_network.retain(|_, msgs| !msgs.is_empty());

        false
    }

    // Call `Node::commit_reconfiguration` for nodes that have prepared and have
    // not yet acked their commit.
    fn action_commit(&mut self, indices: Vec<Index>) -> bool {
        let rack_id = self.nexus.rack_id;
        let latest_config = self.nexus.latest_config();
        if latest_config.op != NexusOp::Committed {
            return true;
        }
        let committable: Vec<_> = latest_config
            .prepared_members
            .difference(&latest_config.committed_members)
            .collect();

        if committable.is_empty() {
            // All members have committed
            self.skipped_actions += 1;
            return true;
        }

        // We shouldn't be calling commit twice or sending multiple replies
        // to nexus, but a random bunch of indices might result in that. We
        // therefore track nodes that have committed already.
        let mut committed: BTreeSet<PlatformId> = BTreeSet::new();

        for index in indices {
            let id = *index.get(&committable);
            if committed.contains(id) {
                continue;
            }
            let (node, ctx) =
                self.sut.nodes.get_mut(id).expect("destination exists");
            node.commit_configuration(ctx, rack_id, latest_config.epoch)
                .expect("commit succeeded");
            committed.insert(id.clone());
        }

        let epoch = latest_config.epoch;
        for from in committed {
            self.underlay_network.push(NexusReply::CommitAck { from, epoch });
        }
        false
    }

    fn action_deliver_nexus_replies(&mut self, n: usize) -> bool {
        let mut config = self.nexus.latest_config_mut();
        let n = usize::min(n, self.underlay_network.len());
        for reply in self.underlay_network.drain(0..n) {
            match reply {
                NexusReply::CommitAck { from, epoch } => {
                    if config.epoch == epoch {
                        config.committed_members.insert(from);
                    }
                }
            }
        }
        false
    }

    /// Poll the coordinator for acks if nexus is preparing, and commit
    /// if enough acks have been received.
    fn action_poll_prepare_acks(&mut self) -> bool {
        let mut latest_config = self.nexus.latest_config_mut();
        if latest_config.op != NexusOp::Preparing {
            // No point in checking. Commit or abort has occurred.
            return true;
        }

        // If the coordinator has crashed then Nexus should abort.
        // Crashing is not actually implemented yet, but it will be.
        if self.faults.crashed_nodes.contains(&latest_config.coordinator) {
            latest_config.op = NexusOp::Aborted;
        }

        // Lookup the coordinator node
        let (coordinator, ctx) = self
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
            return true;
        }

        // Poll the coordinator for acks.
        //
        // We expect the coordinator to still be coordinating since it has not
        // crashed and nexus is still preparing.
        //
        // In a real system this request would go over the network, but would
        // end up at the same place. It's not apparent that its worth the
        // complexity here to delay poll replies to Nexus, but we can do that
        // if necessary and then deliver them when the `DeliverNexusReplies`
        // action fires.
        let cs = coordinator
            .get_coordinator_state()
            .expect("coordinator is coordinating");

        latest_config.prepared_members.extend(cs.op().acked_prepares());

        // Commit if possible
        if latest_config.can_commit() {
            info!(self.log, "nexus committed";
                  "epoch" => %latest_config.epoch,
                  "coordinator" => %latest_config.coordinator
            );

            latest_config.op = NexusOp::Committed;

            let new_members = latest_config.members.clone();
            let new_epoch = latest_config.epoch;

            // Expunge any removed nodes from the last committed configuration
            if let Some(last_committed_epoch) =
                latest_config.last_committed_epoch
            {
                // Release our mutable borrow
                drop(latest_config);

                let last_committed_config = self
                    .nexus
                    .configs
                    .get(&last_committed_epoch)
                    .expect("config exists");

                let expunged = last_committed_config
                    .members
                    .difference(&new_members)
                    .cloned();

                for e in expunged {
                    info!(
                        self.log,
                        "expunged node";
                        "epoch" => %new_epoch,
                        "platform_id" => %e);
                    self.expunged.insert(e);
                }
            }
        }
        false
    }

    fn action_reconfigure(
        &mut self,
        num_added_nodes: usize,
        removed_nodes: Vec<Selector>,
        threshold: Index,
        coordinator: Selector,
    ) -> bool {
        let latest_epoch = self.nexus.latest_config().epoch;
        let last_committed_config = self.nexus.last_committed_config();
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
                        - self.expunged.len(),
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
                    return true;
                }
                let coordinator =
                    coordinator.select(c.committed_members.iter());

                // First, find the set of nodes to remove
                let mut nodes_to_remove = BTreeSet::new();
                for s in removed_nodes {
                    // The same selection can be chosen more than once. so we
                    // must add the extra check rather than shrinking the length
                    // of the `removed_nodes` iterator with `take`.;
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
                for id in self.member_universe.iter() {
                    if nodes_to_add.len() == num_nodes_to_add {
                        break;
                    }
                    if !self.expunged.contains(id) && !c.members.contains(id) {
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
                    return true;
                }
                // Pick the first `num_added_nodes` from member_universe
                // It's as good a choice as any and deterministic
                let new_members: BTreeSet<_> = self
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
        self.nexus.configs.insert_unique(nexus_config).expect("new config");
        self.send_reconfigure_msg();
        false
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
        Ok(())
    }

    /// If a node has a given configuration it must match all other nodes with
    /// that configuration.
    ///
    /// Sometimes nodes may not have a configuration for a given epoch.
    fn invariant_all_nodes_have_same_configuration_per_epoch(
        &self,
    ) -> Result<(), TestCaseError> {
        for (id, (_, ctx)) in &self.sut.nodes {
            let diff = self
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
            let latest_config = self.nexus.latest_config();
            let (node, _) = self
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
            let (_, ctx) = self.sut.nodes.get(&id).expect("node exists");
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
        let latest_config = self.nexus.latest_config();
        if latest_config.op != NexusOp::Committed {
            return Ok(());
        }

        for id in &latest_config.committed_members {
            let (_, ctx) = self.sut.nodes.get(&id).expect("node exists");
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
        for (id, (node, _)) in &self.sut.nodes {
            prop_assert!(
                !(node.get_coordinator_state().is_some()
                    && node.is_computing_key_share()),
                "Coordinating and computing key share on node {}",
                id
            );
        }

        Ok(())
    }
}

/// Broken out of `TestState` to alleviate borrow checker woes
fn send_envelopes(
    ctx: &mut NodeCtx,
    bootstrap_network: &mut BTreeMap<PlatformId, Vec<Envelope>>,
) {
    for envelope in ctx.drain_envelopes() {
        let envelopes =
            bootstrap_network.entry(envelope.to.clone()).or_default();
        envelopes.push(envelope);
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
    ///
    /// Note that we intentionally set the max set size to MAX_CLUSTER_SIZE-1.
    /// This is because we always want to include the coordinator in the
    /// configuration, but its value may not be chosen randomly. In this case,
    /// we have to add it to the actual membership set we generate from this
    /// configuration with [`TestState::generated_config_to_reconfigure_msg`].
    #[strategy(btree_set(0..=MEMBER_UNIVERSE_SIZE, MIN_CLUSTER_SIZE..MAX_CLUSTER_SIZE))]
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
    (0..=MEMBER_UNIVERSE_SIZE)
        .map(|serial| PlatformId::new("test".into(), serial.to_string()))
        .collect()
}

#[derive(Debug, Arbitrary)]
pub struct TestInput {
    initial_config: GeneratedConfiguration,

    // We choose a set of nodes to be crashed, resulting in them being
    // disconnected from every other node.
    #[strategy(btree_set(0..=MEMBER_UNIVERSE_SIZE, 0..MAX_INITIAL_DOWN_NODES))]
    initial_down_nodes: BTreeSet<usize>,
    #[any(size_range(MIN_ACTIONS..MAX_ACTIONS).lift())]
    actions: Vec<Action>,
}

#[proptest]
fn test_trust_quorum_protocol(input: TestInput) {
    let logctx = test_setup_log("test_trust_quorum_protocol");

    let mut state = TestState::new(logctx.log.clone());

    // Perform the initial setup
    state.create_nexus_initial_config(input.initial_config);
    state.setup_initial_connections(input.initial_down_nodes);
    state.send_reconfigure_msg();

    // Check the results of the initial setup
    state.postcondition_initial_configuration()?;

    // Put the coordinator's outgoing messages on the wire if there are any
    state.send_envelopes_from_coordinator();

    // Start executing the actions
    state.run_actions(input.actions)?;

    info!(
        state.log,
        "Test complete";
        "skipped_actions" => state.skipped_actions
    );

    //    logctx.cleanup_successful();
}
