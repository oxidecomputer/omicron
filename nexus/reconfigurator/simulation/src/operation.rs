// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Operations in the simulator's history.
//!
//! Operations track how the set of heads changes over time, similar to
//! `jj op log`. This enables undo, redo, and restore functionality.

use chrono::{DateTime, Utc};
use indexmap::IndexSet;
use omicron_uuid_kinds::{ReconfiguratorSimOpUuid, ReconfiguratorSimStateUuid};

use crate::{Simulator, utils::DisplayUuidPrefix};

/// A single operation in the simulator's history.
#[derive(Clone, Debug)]
pub struct SimOperation {
    id: ReconfiguratorSimOpUuid,
    heads: IndexSet<ReconfiguratorSimStateUuid>,
    current: ReconfiguratorSimStateUuid,
    kind: SimOperationKind,
    timestamp: DateTime<Utc>,
}

impl SimOperation {
    /// Create the root operation.
    pub(crate) fn root() -> Self {
        Self {
            id: Simulator::ROOT_OPERATION_ID,
            heads: IndexSet::new(),
            current: Simulator::ROOT_ID,
            kind: SimOperationKind::Initialize,
            timestamp: Utc::now(),
        }
    }

    /// Create a new operation.
    pub(crate) fn new(
        id: ReconfiguratorSimOpUuid,
        heads: IndexSet<ReconfiguratorSimStateUuid>,
        current: ReconfiguratorSimStateUuid,
        kind: SimOperationKind,
    ) -> Self {
        Self { id, heads, current, kind, timestamp: Utc::now() }
    }

    /// Get the operation ID.
    pub fn id(&self) -> ReconfiguratorSimOpUuid {
        self.id
    }

    /// Get the heads at this operation.
    pub fn heads(&self) -> &IndexSet<ReconfiguratorSimStateUuid> {
        &self.heads
    }

    /// Get the current state at this operation.
    pub fn current(&self) -> ReconfiguratorSimStateUuid {
        self.current
    }

    /// Get the operation kind.
    pub fn kind(&self) -> &SimOperationKind {
        &self.kind
    }

    /// Get the timestamp.
    pub fn timestamp(&self) -> DateTime<Utc> {
        self.timestamp
    }

    /// Format a description of this operation for display.
    pub fn description(&self, verbose: bool) -> String {
        match &self.kind {
            SimOperationKind::Initialize => "initialize simulator".to_string(),
            SimOperationKind::StateAdd { description, .. } => {
                format!("add state: {}", description)
            }
            SimOperationKind::StateSwitch { state_id } => {
                format!(
                    "switch to state: {}",
                    DisplayUuidPrefix::new(*state_id, verbose)
                )
            }
            SimOperationKind::Restore { target: to, kind } => {
                let prefix = match kind {
                    RestoreKind::Undo => "undo to",
                    RestoreKind::Redo => "redo to",
                    RestoreKind::Explicit => "restore to",
                };
                format!("{} {}", prefix, DisplayUuidPrefix::new(*to, verbose))
            }
        }
    }
}

/// The kind of restore operation, part of [`SimOperationKind`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RestoreKind {
    /// An undo operation, moving backward in time.
    Undo,
    /// A redo operation, moving forward in time.
    Redo,
    /// An explicit restore to a specific operation.
    Explicit,
}

/// The kind of [`SimOperation`].
#[derive(Clone, Debug)]
pub enum SimOperationKind {
    /// Initial operation.
    Initialize,

    /// Add a new state.
    StateAdd { state_id: ReconfiguratorSimStateUuid, description: String },

    /// Switch to an existing state.
    StateSwitch { state_id: ReconfiguratorSimStateUuid },

    /// Restore to a target operation.
    Restore { target: ReconfiguratorSimOpUuid, kind: RestoreKind },
}

impl SimOperationKind {
    pub(crate) fn is_undo(&self) -> bool {
        matches!(
            self,
            SimOperationKind::Restore { kind: RestoreKind::Undo, .. }
        )
    }
}
