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
use renderdag::{Ancestor, GraphRowRenderer, Renderer};
use swrite::{SWrite, swrite, swriteln};

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

/// Render the operation log as a graph.
///
/// Operations are shown in reverse chronological order (newest first) as a
/// linear chain (branches are not allowed or supported in the operation graph).
pub(crate) fn render_operation_graph(
    simulator: &Simulator,
    limit: Option<usize>,
    verbose: bool,
) -> String {
    let mut output = String::new();

    let mut renderer = GraphRowRenderer::new()
        .output()
        .with_min_row_height(0)
        .build_box_drawing();

    // limited_ops is in reverse order, so that the most recent operation is
    // shown first.
    let limited_ops: Vec<_> = if let Some(limit) = limit {
        // Take the last `limit` operations.
        simulator.operations().rev().take(limit).collect()
    } else {
        simulator.operations().rev().collect()
    };

    for (op_index, op) in limited_ops.iter().enumerate() {
        let shown_first = op_index == 0;
        let shown_last = op_index == limited_ops.len() - 1;

        let parents = if shown_last {
            Vec::new()
        } else {
            // "next" here is really previous, i.e. next in the reversed list of
            // operations.
            let next_op = limited_ops[op_index + 1];
            vec![Ancestor::Parent(next_op.id())]
        };

        let glyph = if shown_first { "@" } else { "○" };

        let mut message = String::new();
        let timestamp =
            op.timestamp().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        swrite!(
            message,
            "{} {}\n{}",
            DisplayUuidPrefix::new(op.id(), verbose),
            timestamp,
            op.description(verbose),
        );
        // Remove trailing newlines and add one at the end.
        while message.ends_with('\n') {
            message.pop();
        }
        message.push('\n');

        // If verbose is true, show heads at this operation.
        if verbose && !op.heads().is_empty() {
            let current = op.current();

            // Show the current head first with an @ marker.
            if let Some(state) = simulator.get_state(current) {
                swriteln!(
                    message,
                    "  @ {} (gen {})",
                    current,
                    state.generation()
                );
            }

            // Show other heads with *.
            for head in op.heads() {
                if *head != current {
                    if let Some(state) = simulator.get_state(*head) {
                        swriteln!(
                            message,
                            "  * {} (gen {})",
                            head,
                            state.generation()
                        );
                    }
                }
            }
        }

        // We choose to have a more compact view than render_graph here, without
        // a blank line between operations.

        let row = renderer.next_row(op.id(), parents, glyph.into(), message);
        output.push_str(&row);
    }

    output
}
