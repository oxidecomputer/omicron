// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Graph rendering for simulator states.

use std::collections::{HashMap, HashSet};

use omicron_uuid_kinds::ReconfiguratorSimUuid;
use renderdag::{Ancestor, GraphRowRenderer, Renderer};
use swrite::{SWrite, swrite, swriteln};

use crate::{SimState, Simulator, utils::DisplayUuidPrefix};

/// Options for rendering the state graph.
#[derive(Clone, Debug)]
pub struct GraphRenderOptions {
    verbose: bool,
    limit: Option<usize>,
    from: Option<ReconfiguratorSimUuid>,
    current: ReconfiguratorSimUuid,
}

impl GraphRenderOptions {
    /// Create new render options with the current state.
    pub fn new(current: ReconfiguratorSimUuid) -> Self {
        Self { verbose: false, limit: None, from: None, current }
    }

    /// Set true to show verbose details.
    pub fn with_verbose(mut self, verbose: bool) -> Self {
        self.verbose = verbose;
        self
    }

    /// Set the generation limit.
    pub fn with_limit(mut self, limit: Option<usize>) -> Self {
        self.limit = limit;
        self
    }

    /// Set the starting state for ancestry filtering.
    pub fn with_from(mut self, from: Option<ReconfiguratorSimUuid>) -> Self {
        self.from = from;
        self
    }
}

impl Simulator {
    /// Render the state graph as text.
    pub fn render_graph(&self, options: &GraphRenderOptions) -> String {
        let states = self.collect_states_dfs(options);
        render_graph(states, options)
    }

    /// Collect all states in depth-first order, preserving linear sequences
    /// while showing branches at their merge points.
    fn collect_states_dfs(
        &self,
        options: &GraphRenderOptions,
    ) -> Vec<&SimState> {
        let mut remaining_heads: Vec<ReconfiguratorSimUuid> =
            if let Some(from_id) = options.from {
                vec![from_id]
            } else {
                self.heads().iter().copied().collect()
            };

        // Precompute which heads can reach each node. This allows us to find
        // merge points (nodes reachable from multiple heads).
        let mut node_to_heads: HashMap<_, Vec<_>> = HashMap::new();
        for head_id in &remaining_heads {
            let mut current = *head_id;
            loop {
                node_to_heads.entry(current).or_default().push(*head_id);
                match self.get_state(current).and_then(|s| s.parent()) {
                    Some(parent_id) => current = parent_id,
                    None => break,
                }
            }
        }

        // Sort the heads so that one of the heads containing the current state
        // comes last (this is in reverse order so that .pop() returns it
        // first).
        remaining_heads.sort_by_key(|head_id| {
            node_to_heads
                .get(&options.current)
                .map(|heads| heads.contains(&head_id))
                .unwrap_or(false)
        });

        let mut walk_state = WalkState::new(remaining_heads);

        while let Some(start_id) = walk_state.remaining_heads.pop() {
            self.walk_branch_recursive(
                start_id,
                &node_to_heads,
                &mut walk_state,
            );
        }

        walk_state.states
    }

    /// Recursively walk a branch, processing merge points along the way.
    fn walk_branch_recursive<'a>(
        &'a self,
        mut current_id: ReconfiguratorSimUuid,
        node_to_heads: &HashMap<
            ReconfiguratorSimUuid,
            Vec<ReconfiguratorSimUuid>,
        >,
        walk_state: &mut WalkState<'a>,
    ) {
        loop {
            if walk_state.visited.contains(&current_id) {
                break;
            }

            walk_state.visited.insert(current_id);
            let state = self.get_state(current_id).expect("state should exist");
            walk_state.states.push(state);

            if let Some(parent_id) = state.parent() {
                // If there are any branches that merge at the parent, then
                // process them before continuing.
                let merging: Vec<_> = node_to_heads[&parent_id]
                    .iter()
                    .filter(|h| walk_state.remaining_heads.contains(h))
                    .copied()
                    .collect();

                for other_head in merging {
                    walk_state.remaining_heads.retain(|&h| h != other_head);
                    self.walk_branch_recursive(
                        other_head,
                        node_to_heads,
                        walk_state,
                    );
                }

                current_id = parent_id;
            } else {
                break;
            }
        }
    }
}

/// State accumulated during graph traversal.
struct WalkState<'a> {
    states: Vec<&'a SimState>,
    visited: HashSet<ReconfiguratorSimUuid>,
    remaining_heads: Vec<ReconfiguratorSimUuid>,
}

impl<'a> WalkState<'a> {
    fn new(heads: Vec<ReconfiguratorSimUuid>) -> Self {
        Self {
            states: Vec::new(),
            visited: HashSet::new(),
            remaining_heads: heads,
        }
    }
}

/// Render a state graph using sapling-renderdag.
fn render_graph(
    states: Vec<&SimState>,
    options: &GraphRenderOptions,
) -> String {
    let mut output = String::new();

    let mut renderer = GraphRowRenderer::new()
        .output()
        .with_min_row_height(0)
        .build_box_drawing();

    // Apply the limit if specified.
    let limited_states = if let Some(limit) = options.limit {
        &states[..limit.min(states.len())]
    } else {
        &states[..]
    };

    for (index, state) in limited_states.iter().enumerate() {
        let is_last = index == limited_states.len() - 1;

        let parents = match state.parent() {
            Some(parent_id) => vec![Ancestor::Parent(parent_id)],
            None => Vec::new(),
        };

        let glyph = if state.id() == options.current { "@" } else { "○" };

        let mut message = format_message(&state, options);
        // Ensure there's exactly two newlines at the end for most commits, and
        // one for the last one. This results in a single blank line between
        // commit messages.
        while message.ends_with('\n') {
            message.pop();
        }
        message.push('\n');
        if !is_last {
            message.push('\n');
        }

        let row = renderer.next_row(state.id(), parents, glyph.into(), message);

        output.push_str(&row);
    }

    output
}

fn format_message(state: &SimState, options: &GraphRenderOptions) -> String {
    let mut message = String::new();

    swriteln!(
        message,
        "{} (generation {})",
        DisplayUuidPrefix::new(state.id(), options.verbose),
        state.generation()
    );

    // Description lines.
    swrite!(message, "{}", state.description().trim_end());

    // If verbose, add log details if present.
    if options.verbose {
        let log = state.log();
        if !log.system.is_empty()
            || !log.config.is_empty()
            || !log.rng.is_empty()
        {
            swriteln!(message);
            swriteln!(message, "details:");
            for entry in &log.system {
                swriteln!(message, "  system: {}", entry);
            }
            for entry in &log.config {
                swriteln!(message, "  config: {}", entry);
            }
            for entry in &log.rng {
                swriteln!(message, "  rng: {}", entry);
            }
        }
    }

    message
}
