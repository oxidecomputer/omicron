// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A store of successive reconfigurator states: the main entrypoint for
//! reconfigurator simulation.

use std::{collections::HashMap, sync::Arc};

use indexmap::{IndexMap, IndexSet};
use omicron_uuid_kinds::{
    ReconfiguratorSimOpKind, ReconfiguratorSimOpUuid,
    ReconfiguratorSimStateKind, ReconfiguratorSimStateUuid,
};
use typed_rng::TypedUuidRng;

use crate::{
    RestoreKind, SimOperation, SimOperationKind, SimState,
    errors::{
        OpMatch, OperationError, OperationIdPrefixError, StateIdPrefixError,
        StateMatch,
    },
    seed_from_entropy,
};

/// A store to track reconfigurator states: the main entrypoint for
/// reconfigurator simulation.
///
/// This store has key-based storage for systems and their states, and allows
/// for append-only storage of new states.
///
/// # Implementation notes
///
/// We currently index by UUIDs, but we could index by the hash of the contents
/// and make it a Merkle tree just as well. (We'd have to hook up canonical
/// hashing etc; it's a bunch of work but not too difficult). If there's a
/// compelling reason to use Merkle trees, we should switch over to that.
///
/// Currently, all data within is stored via `Arc` instances to enable cheap
/// cloning. In the future, it would be interesting to store data at the top
/// level, and only store references to it within each `SimState`. (The
/// references could be UUIDs, Rust `&` references, or content-addressed
/// hashes.) This does add some complexity, though, and there isn't a clear
/// benefit at the moment. For more, see the comment within the definition of
/// `system.rs`'s `SimSystem`.
#[derive(Clone, Debug)]
pub struct Simulator {
    log: slog::Logger,
    states: HashMap<ReconfiguratorSimStateUuid, Arc<SimState>>,
    // This state corresponds to `ROOT_ID`.
    //
    // Storing it in the Arc is extremely important! `SimStateBuilder` stores a
    // pointer to the root state to ensure that there's no attempt to compare
    // "unrelated histories". The Simulator struct itself can both be cloned and
    // moved in memory, but because `root_state` is never changed, it always
    // points to the same memory address.
    root_state: Arc<SimState>,
    // Top-level (unversioned) RNG for state UUIDs.
    sim_uuid_rng: TypedUuidRng<ReconfiguratorSimStateKind>,
    // Append-only operation log tracking how heads change over time, similar to
    // `jj op log`.
    //
    // The last entry is the current operation, and its heads field contains the
    // current set of heads.
    operations: IndexMap<ReconfiguratorSimOpUuid, SimOperation>,
    // RNG for generating operation UUIDs.
    op_uuid_rng: TypedUuidRng<ReconfiguratorSimOpKind>,
}

impl Simulator {
    /// The root ID of the store.
    ///
    /// This is always defined to be the nil UUID, and if queried will always
    /// have a state associated with it.
    pub const ROOT_ID: ReconfiguratorSimStateUuid =
        ReconfiguratorSimStateUuid::nil();

    /// The root operation ID.
    ///
    /// This is always the first operation in the log.
    pub const ROOT_OPERATION_ID: ReconfiguratorSimOpUuid =
        ReconfiguratorSimOpUuid::nil();

    /// Create a new simulator with the given initial seed.
    pub fn new(log: &slog::Logger, seed: Option<String>) -> Self {
        let seed = seed.unwrap_or_else(|| seed_from_entropy());
        Self::new_inner(log, seed)
    }

    fn new_inner(log: &slog::Logger, seed: String) -> Self {
        let log = log.new(slog::o!("component" => "SimStore"));
        // The ReconfiguratorSimStateUuid type used to be ReconfiguratorSimUuid.
        // Retain the old name in the seed for generated ID compatibility.
        let sim_uuid_rng =
            TypedUuidRng::from_seed(&seed, "ReconfiguratorSimUuid");
        let op_uuid_rng =
            TypedUuidRng::from_seed(&seed, "ReconfiguratorSimOpUuid");
        let root_state = SimState::new_root(seed);

        let mut operations = IndexMap::new();
        operations.insert(Self::ROOT_OPERATION_ID, SimOperation::root());

        Self {
            log,
            states: HashMap::new(),
            root_state,
            sim_uuid_rng,
            operations,
            op_uuid_rng,
        }
    }

    /// Get the initial RNG seed.
    ///
    /// Versioned configurations start with this seed, though they may choose
    /// to change it as they go along.
    pub fn initial_seed(&self) -> &str {
        &self.root_state.rng().seed()
    }

    /// Get the current heads of the store.
    #[inline]
    pub fn heads(&self) -> &IndexSet<ReconfiguratorSimStateUuid> {
        self.operation_current().heads()
    }

    /// Get the current state ID.
    #[inline]
    pub fn current(&self) -> ReconfiguratorSimStateUuid {
        self.operation_current().current()
    }

    /// Get the state for the given UUID.
    pub fn get_state(
        &self,
        id: ReconfiguratorSimStateUuid,
    ) -> Option<&SimState> {
        if id == Self::ROOT_ID {
            return Some(&self.root_state);
        }
        Some(&**self.states.get(&id)?)
    }

    /// Get the root state.
    ///
    /// This is equivalent to
    /// [`Self::get_state`]`(`[`Self::ROOT_ID`]`).unwrap()`.
    pub fn root_state(&self) -> &SimState {
        &self.root_state
    }

    /// Get a state by UUID prefix.
    ///
    /// Returns the unique state ID that matches the given prefix.
    /// Returns an error if zero or multiple states match the prefix.
    pub fn get_state_by_prefix(
        &self,
        prefix: &str,
    ) -> Result<ReconfiguratorSimStateUuid, StateIdPrefixError> {
        let mut matching_ids = Vec::new();

        if Self::ROOT_ID.to_string().starts_with(prefix) {
            matching_ids.push(Self::ROOT_ID);
        }

        for id in self.states.keys() {
            if id.to_string().starts_with(prefix) {
                matching_ids.push(*id);
            }
        }

        match matching_ids.len() {
            0 => Err(StateIdPrefixError::NoMatch(prefix.to_string())),
            1 => Ok(matching_ids[0]),
            n => {
                // Sort for deterministic output.
                matching_ids.sort();

                let matches = matching_ids
                    .iter()
                    .map(|id| {
                        let state = self
                            .get_state(*id)
                            .expect("matching ID should have a state");
                        StateMatch {
                            id: *id,
                            generation: state.generation(),
                            description: state.description().to_string(),
                        }
                    })
                    .collect();

                Err(StateIdPrefixError::Ambiguous {
                    prefix: prefix.to_string(),
                    count: n,
                    matches,
                })
            }
        }
    }

    #[inline]
    pub(crate) fn next_sim_uuid(&mut self) -> ReconfiguratorSimStateUuid {
        self.sim_uuid_rng.next()
    }

    /// Render the operation log as a graph.
    ///
    /// Operations are shown in reverse chronological order (newest first) as a
    /// linear chain.
    pub fn render_operation_graph(
        &self,
        limit: Option<usize>,
        verbose: bool,
    ) -> String {
        crate::operation::render_operation_graph(self, limit, verbose)
    }

    pub(crate) fn add_state(&mut self, state: Arc<SimState>) {
        let id = state.id();
        let parent = state.parent();
        let description = state.description().to_string();

        if self.states.insert(id, Arc::clone(&state)).is_some() {
            panic!("ID {id} should be unique and generated by the store");
        }

        // Compute the new heads: remove the parent if it exists, and add the
        // new state. Unlike in source control we don't have a concept of
        // "merges" here, so there's exactly one parent that may need to be
        // removed.
        let mut new_heads = self.operation_current().heads().clone();
        if let Some(parent) = parent {
            new_heads.shift_remove(&parent);
        }
        new_heads.insert(id);

        // Record this operation in the log.
        let op = SimOperation::new(
            self.op_uuid_rng.next(),
            new_heads,
            id,
            SimOperationKind::StateAdd { state_id: id, description },
        );
        self.operations.insert(op.id(), op);

        slog::debug!(
            self.log,
            "committed new state";
            "id" => %id,
            "parent" => ?parent,
        );
    }

    /// Switch to an existing state.
    ///
    /// This creates a new operation that changes the current state pointer
    /// without adding a new state.
    pub fn switch_state(
        &mut self,
        target_id: ReconfiguratorSimStateUuid,
    ) -> Result<(), OperationError> {
        // Verify the target state exists.
        if target_id != Self::ROOT_ID && !self.states.contains_key(&target_id) {
            return Err(OperationError::StateNotFound(target_id));
        }

        // The heads don't change - only the current pointer changes.
        let new_heads = self.operation_current().heads().clone();

        // Create a new SwitchState operation.
        let switch_op = SimOperation::new(
            self.op_uuid_rng.next(),
            new_heads,
            target_id,
            SimOperationKind::StateSwitch { state_id: target_id },
        );
        self.operations.insert(switch_op.id(), switch_op);

        slog::debug!(
            self.log,
            "switched to state";
            "to" => %target_id,
        );

        Ok(())
    }

    /// Get the current operation.
    pub fn operation_current(&self) -> &SimOperation {
        self.operations.last().expect("operations should never be empty").1
    }

    /// Get all operations in the log.
    pub fn operations(&self) -> impl Iterator<Item = &SimOperation> {
        self.operations.values()
    }

    /// Get an operation by ID.
    pub fn operation_get(
        &self,
        id: ReconfiguratorSimOpUuid,
    ) -> Option<&SimOperation> {
        self.operations.get(&id)
    }

    /// Get an operation by UUID prefix.
    ///
    /// Returns the unique operation ID that matches the given prefix.
    /// Returns an error if zero or multiple operations match the prefix.
    pub fn operation_get_by_prefix(
        &self,
        prefix: &str,
    ) -> Result<ReconfiguratorSimOpUuid, OperationIdPrefixError> {
        let mut matching_ids = Vec::new();

        for op in self.operations.values() {
            if op.id().to_string().starts_with(prefix) {
                matching_ids.push(op.id());
            }
        }

        match matching_ids.len() {
            0 => Err(OperationIdPrefixError::NoMatch(prefix.to_string())),
            1 => Ok(matching_ids[0]),
            n => {
                // Sort for deterministic output.
                matching_ids.sort();

                let matches = matching_ids
                    .iter()
                    .map(|id| {
                        let op = self
                            .operation_get(*id)
                            .expect("matching ID should have an operation");
                        OpMatch {
                            id: *id,
                            description: op.description(false),
                            timestamp: op.timestamp(),
                        }
                    })
                    .collect();

                Err(OperationIdPrefixError::Ambiguous {
                    prefix: prefix.to_string(),
                    count: n,
                    matches,
                })
            }
        }
    }

    /// Undo an operation.
    ///
    /// This restores the heads to an earlier state.
    pub fn operation_undo(&mut self) -> Result<(), OperationError> {
        let current_op = self.operation_current();

        // Follow the undo stack algorithm described in
        // https://github.com/jj-vcs/jj/blob/fd5accb/cli/src/commands/undo.rs#L100:
        //
        // - If the operation to undo is a regular one (not an undo-operation), simply
        //   undo it (== restore its parent).
        // - If the operation to undo is an undo-operation itself, undo that operation
        //   to which the previous undo-operation restored the repo.
        // - If the operation to restore to is an undo-operation, restore directly to
        //   the original operation. This avoids creating a linked list of
        //   undo-operations, which subsequently may have to be walked with an
        //   inefficient loop.

        let op_to_undo_index = match current_op.kind() {
            SimOperationKind::Restore { target, kind: RestoreKind::Undo } => {
                self.operations
                    .get_index_of(target)
                    .expect("restored-to operation should exist")
            }
            _ => {
                // The current operation is at self.operations.len() - 1 (there
                // should always be at least one operation).
                self.operations.len() - 1
            }
        };
        if op_to_undo_index == 0 {
            return Err(OperationError::AtRoot);
        }

        let (_, mut op_to_restore) = self
            .operations
            .get_index(op_to_undo_index - 1)
            .expect("op should exist");

        // If target_op is an undo operation, go to its target.
        if let SimOperationKind::Restore { target, kind: RestoreKind::Undo } =
            op_to_restore.kind()
        {
            op_to_restore = self
                .operation_get(*target)
                .expect("target op undo target should exist");
        }

        let target = op_to_restore.id();
        let new_heads = op_to_restore.heads().clone();
        let new_current = op_to_restore.current();

        // Create the undo operation.
        let undo_op = SimOperation::new(
            self.op_uuid_rng.next(),
            new_heads,
            new_current,
            SimOperationKind::Restore { target, kind: RestoreKind::Undo },
        );
        self.operations.insert(undo_op.id(), undo_op);

        Ok(())
    }

    /// Redo a previously undone operation.
    ///
    /// Only works after an undo or redo operation, and only works if there is a
    /// operation to undo available.
    pub fn operation_redo(&mut self) -> Result<(), OperationError> {
        let current_op = self.operation_current();

        // Follow the redo stack algorithm described at
        // https://github.com/jj-vcs/jj/blob/fd5accb/cli/src/commands/redo.rs#L47:
        //
        // - If the operation to redo is a regular one (neither an undo- or
        //   redo-operation): Fail, because there is nothing to redo.
        // - If the operation to redo is an undo-operation, try to redo it (by restoring
        //   its parent operation).
        // - If the operation to redo is a redo-operation itself, redo the operation the
        //   early redo-operation restored to.
        // - If the operation to restore to is a redo-operation itself, restore directly
        //   to the original operation. This avoids creating a linked list of
        //   redo-operations, which subsequently may have to be walked with an
        //   inefficient loop.

        let (op_to_redo_index, op_to_redo) = match current_op.kind() {
            SimOperationKind::Restore { target, kind: RestoreKind::Redo } => {
                let (ix, _, op) = self
                    .operations
                    .get_full(target)
                    .expect("restored-to operation should exist");
                (ix, op)
            }
            _ => {
                // The current operation is at self.operations.len() - 1 (there
                // should always be at least one operation).
                (self.operations.len() - 1, current_op)
            }
        };

        // The operation to redo must be an undo operation. (This is the one
        // difference between the otherwise symmetric undo and redo methods.)
        if !op_to_redo.kind().is_undo() {
            return Err(OperationError::NoRedo);
        }
        assert_ne!(
            op_to_redo_index, 0,
            "operation 0 is never an undo operation"
        );

        let (_, mut op_to_restore) = self
            .operations
            .get_index(op_to_redo_index - 1)
            .expect("op should exist");

        // If target_op is a redo operation, go to its target.
        if let SimOperationKind::Restore { target, kind: RestoreKind::Redo } =
            op_to_restore.kind()
        {
            op_to_restore = self
                .operation_get(*target)
                .expect("target op undo target should exist");
        }

        let target = op_to_restore.id();
        let new_heads = op_to_restore.heads().clone();
        let new_current = op_to_restore.current();

        // Create the redo operation.
        let redo_op = SimOperation::new(
            self.op_uuid_rng.next(),
            new_heads,
            new_current,
            SimOperationKind::Restore { target, kind: RestoreKind::Redo },
        );
        self.operations.insert(redo_op.id(), redo_op);

        Ok(())
    }

    /// Restore to a specific operation by ID.
    ///
    /// This creates a new Restore operation that sets the heads and current
    /// pointers to older values.
    pub fn operation_restore(
        &mut self,
        target: ReconfiguratorSimOpUuid,
    ) -> Result<(), OperationError> {
        let target_op = self
            .operation_get(target)
            .ok_or(OperationError::NotFound(target))?;
        let new_heads = target_op.heads().clone();
        let new_current = target_op.current();

        // Create the restore operation.
        let restore_op = SimOperation::new(
            self.op_uuid_rng.next(),
            new_heads,
            new_current,
            SimOperationKind::Restore { target, kind: RestoreKind::Explicit },
        );
        self.operations.insert(restore_op.id(), restore_op);

        slog::debug!(
            self.log,
            "restored to operation";
            "to" => %target,
        );

        Ok(())
    }

    /// Wipe the operation log and all states, resetting to just the root.
    ///
    /// This completely clears the operation log and the graph of states, and
    /// starts over from the root state.
    pub fn operation_wipe(&mut self) {
        let mut operations = IndexMap::new();
        operations.insert(Self::ROOT_OPERATION_ID, SimOperation::root());
        self.operations = operations;
        self.states.clear();

        slog::debug!(self.log, "wiped states and operation log");
    }
}
