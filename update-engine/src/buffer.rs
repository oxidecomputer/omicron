// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use std::{
    collections::{HashMap, VecDeque},
    fmt,
    sync::Arc,
    time::Duration,
};

use derive_where::derive_where;
use either::Either;
use indexmap::IndexMap;
use petgraph::{prelude::*, visit::Walker};

use crate::{
    display::AbortMessageDisplay,
    events::{
        Event, EventReport, ProgressEvent, ProgressEventKind, StepEvent,
        StepEventKind, StepEventPriority, StepInfo, StepOutcome,
    },
    ExecutionId, NestedSpec, StepSpec,
};

/// A receiver for events that provides a pull-based model with periodic
/// reports.
///
/// By default, the update engine provides a *push-based model* where you are
/// notified of new events as soon as they come in. The event buffer converts
/// events into a *pull-based model*, where periodic, serializable
/// [`EventReport`]s can be generated.
///
/// # Features
///
/// The buffer is responsible for tracking step and progress events as they
/// come in. The buffer can:
///
/// * Receive events and update its internal state based on them.
/// * Discard progress events that are no longer useful.
/// * Cap the number of [low-priority events](StepEventPriority::Low) such that
///   older events are dropped.
///
/// The buffer is currently resilient against:
///
/// * duplicated and dropped progress events
/// * duplicated step events
/// * dropped *low-priority* step events
///
/// The buffer is currently *not* resilient against:
/// * dropped *high-priority* step events
/// * reordered progress or step events
///
/// These cases can be handled on a best-effort basis in the future at some cost
/// to complexity, if required.
#[derive_where(Clone, Debug)]
pub struct EventBuffer<S: StepSpec> {
    event_store: EventStore<S>,
    max_low_priority: usize,
}

impl<S: StepSpec> EventBuffer<S> {
    /// Creates a new event buffer.
    ///
    /// `max_low_priority` determines the maximum number of low-priority events
    /// retained for a particular step at any given time.
    pub fn new(max_low_priority: usize) -> Self {
        Self { event_store: EventStore::default(), max_low_priority }
    }

    /// The default value for `max_low_priority`, as created by EventBuffer::default().
    pub const DEFAULT_MAX_LOW_PRIORITY: usize = 8;

    /// Adds an [`EventReport`] to the buffer.
    pub fn add_event_report(&mut self, report: EventReport<S>) {
        for event in report.step_events {
            self.add_step_event(event);
        }
        for event in report.progress_events {
            self.add_progress_event(event);
        }
    }

    /// Adds an individual [`Event`] to the buffer.
    pub fn add_event(&mut self, event: Event<S>) {
        match event {
            Event::Step(event) => {
                self.add_step_event(event);
            }
            Event::Progress(event) => {
                self.add_progress_event(event);
            }
        }
    }

    /// Adds a [`StepEvent`] to the buffer.
    ///
    /// This might cause older low-priority events to fall off the list.
    pub fn add_step_event(&mut self, event: StepEvent<S>) {
        self.event_store.handle_root_step_event(event, self.max_low_priority);
    }

    /// Returns the root execution ID, if this event buffer is aware of any
    /// events.
    pub fn root_execution_id(&self) -> Option<ExecutionId> {
        self.event_store.root_execution_id
    }

    /// Returns an execution summary for the root execution ID, if this event buffer is aware of any
    /// events.
    pub fn root_execution_summary(&self) -> Option<ExecutionSummary> {
        // XXX: more efficient algorithm
        let root_execution_id = self.root_execution_id()?;
        let mut summary = self.steps().summarize();
        summary.swap_remove(&root_execution_id)
    }

    /// Returns information about each step, as currently tracked by the buffer,
    /// in order of when the events were first defined.
    pub fn steps(&self) -> EventBufferSteps<'_, S> {
        EventBufferSteps::new(&self.event_store)
    }

    /// Returns information about the given step, as currently tracked by the
    /// buffer.
    pub fn get(&self, step_key: &StepKey) -> Option<&EventBufferStepData<S>> {
        self.event_store.map.get(step_key)
    }

    /// Generates an [`EventReport`] for this buffer.
    ///
    /// This report can be serialized and sent over the wire.
    pub fn generate_report(&self) -> EventReport<S> {
        self.generate_report_since(&mut None)
    }

    /// Generates an [`EventReport`] for this buffer, updating `last_seen` to a
    /// new value for incremental report generation.
    ///
    /// This report can be serialized and sent over the wire.
    pub fn generate_report_since(
        &self,
        last_seen: &mut Option<usize>,
    ) -> EventReport<S> {
        // Gather step events across all keys.
        let mut step_events = Vec::new();
        let mut progress_events = Vec::new();
        for (_, step_data) in self.steps().as_slice() {
            step_events
                .extend(step_data.step_events_since_impl(*last_seen).cloned());
            progress_events
                .extend(step_data.step_status.progress_event().cloned());
        }

        // Sort events.
        step_events.sort_unstable_by_key(|event| event.event_index);
        progress_events.sort_unstable_by_key(|event| event.total_elapsed);
        if let Some(last) = step_events.last() {
            // Only update last_seen if there are new step events (otherwise it
            // stays the same).
            *last_seen = Some(last.event_index);
        }

        EventReport {
            step_events,
            progress_events,
            root_execution_id: self.root_execution_id(),
            last_seen: *last_seen,
        }
    }

    /// Returns true if any further step events are pending since `last_seen`.
    ///
    /// This does not currently care about pending progress events, just pending
    /// step events. A typical use for this is to check that all step events
    /// have been reported before a sender shuts down.
    pub fn has_pending_events_since(&self, last_seen: Option<usize>) -> bool {
        for (_, step_data) in self.steps().as_slice() {
            if step_data.step_events_since_impl(last_seen).next().is_some() {
                return true;
            }
        }
        false
    }

    pub fn add_progress_event(&mut self, event: ProgressEvent<S>) {
        self.event_store.handle_progress_event(event);
    }
}

impl<S: StepSpec> Default for EventBuffer<S> {
    fn default() -> Self {
        Self {
            event_store: Default::default(),
            max_low_priority: Self::DEFAULT_MAX_LOW_PRIORITY,
        }
    }
}

#[derive_where(Clone, Debug, Default)]
struct EventStore<S: StepSpec> {
    // A tree which has the general structure:
    //
    // root execution id ───> root step 0
    //     │      │
    //     │      └─────────> root step 1 ───> nested execution id
    //     │                                       │        │
    //     │                                       v        v
    //     │                             nested step 0    nested step 1
    //     │
    //     └────────────────> root step 2
    //
    // and so on.
    //
    // While petgraph seems like overkill at first, it results in really
    // straightforward algorithms below compared to alternatives like storing
    // trees using Box pointers.
    event_tree: DiGraphMap<EventTreeNode, ()>,
    root_execution_id: Option<ExecutionId>,
    map: HashMap<StepKey, EventBufferStepData<S>>,
}

impl<S: StepSpec> EventStore<S> {
    /// Returns a DFS of event map values.
    fn event_map_value_dfs(
        &self,
    ) -> impl Iterator<Item = (StepKey, &EventBufferStepData<S>)> + '_ {
        self.root_execution_id.into_iter().flat_map(|execution_id| {
            let dfs =
                Dfs::new(&self.event_tree, EventTreeNode::Root(execution_id));
            dfs.iter(&self.event_tree).filter_map(|node| {
                if let EventTreeNode::Step(key) = node {
                    Some((key, &self.map[&key]))
                } else {
                    None
                }
            })
        })
    }

    /// Handles a non-nested step event.
    fn handle_root_step_event(
        &mut self,
        event: StepEvent<S>,
        max_low_priority: usize,
    ) {
        if matches!(event.kind, StepEventKind::Unknown) {
            // Ignore unknown events.
            return;
        }

        // This is a non-nested step event so the event index is a root event
        // index.
        let root_event_index = RootEventIndex(event.event_index);

        let actions = self.recurse_for_step_event(
            &event,
            0,
            None,
            None,
            root_event_index,
            event.total_elapsed,
        );

        if let Some(new_execution) = actions.new_execution {
            if new_execution.nest_level == 0 {
                self.root_execution_id = Some(new_execution.execution_id);
            }

            if let Some((first_step_key, ..)) =
                new_execution.steps_to_add.first()
            {
                // Do we already know about this execution? If so, grab the parent
                // key and child index from the first step.
                let parent_key_and_child_index =
                    if let Some(data) = self.map.get(first_step_key) {
                        data.parent_key_and_child_index
                    } else {
                        if let Some(parent_key) = new_execution.parent_key {
                            match self.map.get_mut(&parent_key) {
                                Some(parent_data) => {
                                    let child_index =
                                        parent_data.child_executions_seen;
                                    parent_data.child_executions_seen += 1;
                                    Some((parent_key, child_index))
                                }
                                None => {
                                    // This should never happen -- it indicates that the
                                    // parent key was unknown. This can happen if we
                                    // didn't receive an event regarding a parent
                                    // execution being started.
                                    None
                                }
                            }
                        } else {
                            None
                        }
                    };

                let total_steps = new_execution.steps_to_add.len();
                for (new_step_key, new_step, sort_key) in
                    new_execution.steps_to_add
                {
                    // These are brand new steps so their keys shouldn't exist in the
                    // map. But if they do, don't overwrite them.
                    self.map.entry(new_step_key).or_insert_with(|| {
                        EventBufferStepData::new(
                            new_step,
                            parent_key_and_child_index,
                            sort_key,
                            new_execution.nest_level,
                            total_steps,
                            root_event_index,
                        )
                    });
                }
            }
        }

        if let Some(key) = actions.progress_key {
            if let Some(value) = self.map.get_mut(&key) {
                // Set progress *before* adding the step event so that it can
                // transition to the running state if it isn't there already.
                if let Some(current_progress) = event.progress_event() {
                    value.set_progress(current_progress);
                }
            }
        }

        if let Some(key) = actions.step_key {
            if let Some(value) = self.map.get_mut(&key) {
                match event.kind.priority() {
                    StepEventPriority::High => {
                        value.add_high_priority_step_event(event);
                    }
                    StepEventPriority::Low => {
                        value.add_low_priority_step_event(
                            event,
                            max_low_priority,
                        );
                    }
                }
            }
        }
    }

    fn handle_progress_event(&mut self, event: ProgressEvent<S>) {
        if matches!(event.kind, ProgressEventKind::Unknown) {
            // Ignore unknown events.
            return;
        }

        if let Some(key) = Self::step_key_for_progress_event(&event) {
            if let Some(value) = self.map.get_mut(&key) {
                value.set_progress(event);
            }
        }
    }

    /// Recurses down the structure of a step event, adding nodes to the event
    /// tree as required. Returns the event key for the next event, if one is
    /// available.
    fn recurse_for_step_event<S2: StepSpec>(
        &mut self,
        event: &StepEvent<S2>,
        nest_level: usize,
        parent_key: Option<StepKey>,
        parent_sort_key: Option<&StepSortKey>,
        root_event_index: RootEventIndex,
        root_total_elapsed: Duration,
    ) -> RecurseActions {
        let mut new_execution = None;
        let (step_key, progress_key) = match &event.kind {
            StepEventKind::ExecutionStarted { steps, first_step, .. } => {
                let root_node = EventTreeNode::Root(event.execution_id);
                self.add_root_node(event.execution_id);
                // All nodes are added during the ExecutionStarted phase.
                let mut steps_to_add = Vec::new();
                for step in steps {
                    let step_key = StepKey {
                        execution_id: event.execution_id,
                        index: step.index,
                    };
                    let sort_key = StepSortKey::new(
                        parent_sort_key,
                        root_event_index.0,
                        step.index,
                    );
                    let step_node = self.add_step_node(step_key);
                    self.event_tree.add_edge(root_node, step_node, ());
                    let step_info = step.clone().into_generic();
                    steps_to_add.push((step_key, step_info, sort_key));
                }
                new_execution = Some(NewExecutionAction {
                    execution_id: event.execution_id,
                    parent_key,
                    nest_level,
                    steps_to_add,
                });

                // Register the start of progress.
                let key = StepKey {
                    execution_id: event.execution_id,
                    index: first_step.info.index,
                };
                (Some(key), Some(key))
            }
            StepEventKind::StepCompleted {
                step,
                attempt,
                outcome,
                next_step,
                step_elapsed,
                attempt_elapsed,
                ..
            } => {
                let key = StepKey {
                    execution_id: event.execution_id,
                    index: step.info.index,
                };
                let outcome = outcome.clone().into_generic();
                let info = CompletionInfo {
                    attempt: *attempt,
                    outcome,
                    root_total_elapsed,
                    leaf_total_elapsed: event.total_elapsed,
                    step_elapsed: *step_elapsed,
                    attempt_elapsed: *attempt_elapsed,
                };
                // Mark this key and all child keys completed.
                self.mark_step_key_completed(key, info, root_event_index);

                // Register the next step in the event map.
                let next_key = StepKey {
                    execution_id: event.execution_id,
                    index: next_step.info.index,
                };
                (Some(key), Some(next_key))
            }
            StepEventKind::ProgressReset { step, .. }
            | StepEventKind::AttemptRetry { step, .. } => {
                // Reset progress for the step in the event map.
                let key = StepKey {
                    execution_id: event.execution_id,
                    index: step.info.index,
                };
                (Some(key), Some(key))
            }
            StepEventKind::ExecutionCompleted {
                last_step: step,
                last_attempt,
                last_outcome,
                step_elapsed,
                attempt_elapsed,
            } => {
                // This is a terminal event: clear all progress for this
                // execution ID and any nested events.

                let key = StepKey {
                    execution_id: event.execution_id,
                    index: step.info.index,
                };
                let outcome = last_outcome.clone().into_generic();
                let info = CompletionInfo {
                    attempt: *last_attempt,
                    outcome,
                    root_total_elapsed,
                    leaf_total_elapsed: event.total_elapsed,
                    step_elapsed: *step_elapsed,
                    attempt_elapsed: *attempt_elapsed,
                };
                // Mark this key and all child keys completed.
                self.mark_execution_id_completed(key, info, root_event_index);

                (Some(key), Some(key))
            }
            StepEventKind::ExecutionFailed {
                failed_step: step,
                total_attempts,
                step_elapsed,
                attempt_elapsed,
                message,
                causes,
            } => {
                // This is a terminal event: clear all progress for this
                // execution ID and any nested events.

                let key = StepKey {
                    execution_id: event.execution_id,
                    index: step.info.index,
                };
                let info = FailureInfo {
                    total_attempts: *total_attempts,
                    message: message.clone(),
                    causes: causes.clone(),
                    root_total_elapsed,
                    leaf_total_elapsed: event.total_elapsed,
                    step_elapsed: *step_elapsed,
                    attempt_elapsed: *attempt_elapsed,
                };
                self.mark_step_failed(key, info, root_event_index);

                (Some(key), Some(key))
            }
            StepEventKind::ExecutionAborted {
                aborted_step: step,
                attempt,
                step_elapsed,
                attempt_elapsed,
                message,
            } => {
                // This is a terminal event: clear all progress for this
                // execution ID and any nested events.

                let key = StepKey {
                    execution_id: event.execution_id,
                    index: step.info.index,
                };
                let info = AbortInfo {
                    attempt: *attempt,
                    message: message.clone(),
                    root_total_elapsed,
                    leaf_total_elapsed: event.total_elapsed,
                    step_elapsed: *step_elapsed,
                    attempt_elapsed: *attempt_elapsed,
                };
                self.mark_step_aborted(key, info, root_event_index);

                (Some(key), Some(key))
            }
            StepEventKind::Nested { step, event: nested_event, .. } => {
                // Recurse and find any nested events.
                let parent_key = StepKey {
                    execution_id: event.execution_id,
                    index: step.info.index,
                };

                // The parent should always exist, but if it doesn't dont fail on that.
                let parent_sort_key = self
                    .map
                    .get(&parent_key)
                    .map(|data| data.sort_key().clone());

                let actions = self.recurse_for_step_event(
                    nested_event,
                    nest_level + 1,
                    Some(parent_key),
                    parent_sort_key.as_ref(),
                    root_event_index,
                    root_total_elapsed,
                );
                if let Some(nested_new_execution) = &actions.new_execution {
                    // Add an edge from the parent node to the new execution's root node.
                    self.event_tree.add_edge(
                        EventTreeNode::Step(parent_key),
                        EventTreeNode::Root(nested_new_execution.execution_id),
                        (),
                    );
                }

                new_execution = actions.new_execution;
                (actions.step_key, actions.progress_key)
            }
            StepEventKind::NoStepsDefined | StepEventKind::Unknown => {
                (None, None)
            }
        };

        RecurseActions { new_execution, step_key, progress_key }
    }

    fn step_key_for_progress_event<S2: StepSpec>(
        event: &ProgressEvent<S2>,
    ) -> Option<StepKey> {
        match &event.kind {
            ProgressEventKind::WaitingForProgress { step, .. }
            | ProgressEventKind::Progress { step, .. } => {
                let key = StepKey {
                    execution_id: event.execution_id,
                    index: step.info.index,
                };
                Some(key)
            }
            ProgressEventKind::Nested { event: nested_event, .. } => {
                Self::step_key_for_progress_event(nested_event)
            }
            ProgressEventKind::Unknown => None,
        }
    }

    fn add_root_node(&mut self, execution_id: ExecutionId) -> EventTreeNode {
        self.event_tree.add_node(EventTreeNode::Root(execution_id))
    }

    fn add_step_node(&mut self, key: StepKey) -> EventTreeNode {
        self.event_tree.add_node(EventTreeNode::Step(key))
    }

    fn mark_step_key_completed(
        &mut self,
        root_key: StepKey,
        info: CompletionInfo,
        root_event_index: RootEventIndex,
    ) {
        let info = Arc::new(info);
        if let Some(value) = self.map.get_mut(&root_key) {
            // Completion status only applies to the root key. Nodes reachable
            // from this node are still marked as complete, but without status.
            value.mark_completed(
                CompletionReason::StepCompleted(info.clone()),
                root_event_index,
            );
        }

        // Mark anything reachable from this node as completed.
        let mut dfs =
            DfsPostOrder::new(&self.event_tree, EventTreeNode::Step(root_key));
        while let Some(key) = dfs.next(&self.event_tree) {
            if let EventTreeNode::Step(key) = key {
                if key != root_key {
                    if let Some(value) = self.map.get_mut(&key) {
                        value.mark_completed(
                            CompletionReason::ParentCompleted {
                                parent_step: root_key,
                                parent_info: info.clone(),
                            },
                            root_event_index,
                        );
                    }
                }
            }
        }
    }

    fn mark_execution_id_completed(
        &mut self,
        root_key: StepKey,
        info: CompletionInfo,
        root_event_index: RootEventIndex,
    ) {
        let info = Arc::new(info);
        if let Some(value) = self.map.get_mut(&root_key) {
            // Completion status only applies to the root key.
            value.mark_completed(
                CompletionReason::StepCompleted(info.clone()),
                root_event_index,
            );
        }

        let mut dfs = DfsPostOrder::new(
            &self.event_tree,
            EventTreeNode::Root(root_key.execution_id),
        );
        while let Some(key) = dfs.next(&self.event_tree) {
            if let EventTreeNode::Step(key) = key {
                if key != root_key {
                    if let Some(value) = self.map.get_mut(&key) {
                        // There's two kinds of nodes reachable from
                        // EventTreeNode::Root that could be marked as
                        // completed: subsequent steps within the same
                        // execution, and steps in child executions.
                        if key.execution_id == root_key.execution_id {
                            value.mark_completed(
                                CompletionReason::SubsequentStarted {
                                    later_step: root_key,
                                    root_total_elapsed: info.root_total_elapsed,
                                },
                                root_event_index,
                            );
                        } else {
                            value.mark_completed(
                                CompletionReason::ParentCompleted {
                                    parent_step: root_key,
                                    parent_info: info.clone(),
                                },
                                root_event_index,
                            );
                        }
                    }
                }
            }
        }
    }

    fn mark_step_failed(
        &mut self,
        root_key: StepKey,
        info: FailureInfo,
        root_event_index: RootEventIndex,
    ) {
        let info = Arc::new(info);
        self.mark_step_failed_impl(root_key, |value, kind| {
            match kind {
                MarkStepFailedImplKind::Root => {
                    value.mark_failed(
                        FailureReason::StepFailed(info.clone()),
                        root_event_index,
                    );
                }
                MarkStepFailedImplKind::Descendant => {
                    value.mark_failed(
                        FailureReason::ParentFailed {
                            parent_step: root_key,
                            parent_info: info.clone(),
                        },
                        root_event_index,
                    );
                }
                MarkStepFailedImplKind::Subsequent => {
                    value.mark_will_not_be_run(
                        WillNotBeRunReason::PreviousStepFailed {
                            step: root_key,
                        },
                        root_event_index,
                    );
                }
                MarkStepFailedImplKind::PreviousCompleted => {
                    value.mark_completed(
                        CompletionReason::SubsequentStarted {
                            later_step: root_key,
                            root_total_elapsed: info.root_total_elapsed,
                        },
                        root_event_index,
                    );
                }
            };
        })
    }

    fn mark_step_aborted(
        &mut self,
        root_key: StepKey,
        info: AbortInfo,
        root_event_index: RootEventIndex,
    ) {
        let info = Arc::new(info);
        self.mark_step_failed_impl(root_key, |value, kind| {
            match kind {
                MarkStepFailedImplKind::Root => {
                    value.mark_aborted(
                        AbortReason::StepAborted(info.clone()),
                        root_event_index,
                    );
                }
                MarkStepFailedImplKind::Descendant => {
                    value.mark_aborted(
                        AbortReason::ParentAborted {
                            parent_step: root_key,
                            parent_info: info.clone(),
                        },
                        root_event_index,
                    );
                }
                MarkStepFailedImplKind::Subsequent => {
                    value.mark_will_not_be_run(
                        WillNotBeRunReason::PreviousStepAborted {
                            step: root_key,
                        },
                        root_event_index,
                    );
                }
                MarkStepFailedImplKind::PreviousCompleted => {
                    value.mark_completed(
                        CompletionReason::SubsequentStarted {
                            later_step: root_key,
                            root_total_elapsed: info.root_total_elapsed,
                        },
                        root_event_index,
                    );
                }
            };
        });
    }

    fn mark_step_failed_impl(
        &mut self,
        root_key: StepKey,
        mut cb: impl FnMut(&mut EventBufferStepData<S>, MarkStepFailedImplKind),
    ) {
        if let Some(value) = self.map.get_mut(&root_key) {
            (cb)(value, MarkStepFailedImplKind::Root);
        }

        // Exceptional situation (in normal use, past steps should always show
        // up): Mark all past steps for this key as completed. The assumption
        // here is that this is the first step that failed.
        for index in 0..root_key.index {
            let key = StepKey { execution_id: root_key.execution_id, index };
            if let Some(value) = self.map.get_mut(&key) {
                (cb)(value, MarkStepFailedImplKind::PreviousCompleted);
            }
        }

        // Exceptional situation (in normal use, descendant steps should always
        // show up if they aren't being run): Mark all descendant steps as
        // failed -- there isn't enough else to go by.
        let mut dfs =
            DfsPostOrder::new(&self.event_tree, EventTreeNode::Step(root_key));
        while let Some(key) = dfs.next(&self.event_tree) {
            if let EventTreeNode::Step(key) = key {
                if let Some(value) = self.map.get_mut(&key) {
                    (cb)(value, MarkStepFailedImplKind::Descendant);
                }
            }
        }

        // Mark all future steps for this execution ID as "will not be run", We
        // do this last because all non-future steps for this execution ID will
        // have been covered by the above loops.
        let mut dfs = DfsPostOrder::new(
            &self.event_tree,
            EventTreeNode::Root(root_key.execution_id),
        );
        while let Some(key) = dfs.next(&self.event_tree) {
            if let EventTreeNode::Step(key) = key {
                if let Some(value) = self.map.get_mut(&key) {
                    (cb)(value, MarkStepFailedImplKind::Subsequent);
                }
            }
        }
    }
}

enum MarkStepFailedImplKind {
    Root,
    Descendant,
    Subsequent,
    PreviousCompleted,
}

/// Actions taken by a recursion step.
#[derive(Clone, Debug)]
struct RecurseActions {
    new_execution: Option<NewExecutionAction>,
    // The key to record this step against.
    step_key: Option<StepKey>,
    // The key to record the progress action against.
    progress_key: Option<StepKey>,
}

#[derive(Clone, Debug)]
struct NewExecutionAction {
    // An execution ID corresponding to a new run, if seen.
    execution_id: ExecutionId,

    // The parent key for this execution, if this is a nested step.
    parent_key: Option<StepKey>,

    // The nest level for this execution.
    nest_level: usize,

    // New steps to add, generated by ExecutionStarted events.
    // The tuple is:
    // * step key
    // * step info
    // * step sort key
    steps_to_add: Vec<(StepKey, StepInfo<NestedSpec>, StepSortKey)>,
}

/// An ordered list of steps contained in an event buffer.
///
/// Returned by [`EventBuffer::steps`].
#[derive_where(Clone, Debug)]
pub struct EventBufferSteps<'buf, S: StepSpec> {
    steps: Vec<(StepKey, &'buf EventBufferStepData<S>)>,
}

impl<'buf, S: StepSpec> EventBufferSteps<'buf, S> {
    fn new(event_store: &'buf EventStore<S>) -> Self {
        let mut steps: Vec<_> = event_store.event_map_value_dfs().collect();
        steps.sort_unstable_by_key(|(_, value)| value.sort_key());
        Self { steps }
    }

    /// Returns the list of steps in the event buffer.
    pub fn as_slice(&self) -> &[(StepKey, &'buf EventBufferStepData<S>)] {
        &self.steps
    }

    /// Summarizes the current state of all known executions, keyed by execution
    /// ID.
    ///
    /// Values are returned as an `IndexMap`, in order of when execution IDs
    /// were first defined.
    pub fn summarize(&self) -> IndexMap<ExecutionId, ExecutionSummary> {
        let mut by_execution_id: IndexMap<ExecutionId, Vec<_>> =
            IndexMap::new();
        // Index steps by execution key.
        for &(step_key, data) in &self.steps {
            by_execution_id
                .entry(step_key.execution_id)
                .or_default()
                .push(data);
        }

        by_execution_id
            .into_iter()
            .map(|(execution_id, steps)| {
                let summary = ExecutionSummary::new(execution_id, &steps);
                (execution_id, summary)
            })
            .collect()
    }
}

/// Step-related data for a particular key.
#[derive_where(Clone, Debug)]
pub struct EventBufferStepData<S: StepSpec> {
    step_info: StepInfo<NestedSpec>,

    sort_key: StepSortKey,

    // TODO: These steps are common to each execution, but are stored separately
    // here. These should likely move into EventBufferExecutionData.
    parent_key_and_child_index: Option<(StepKey, usize)>,
    nest_level: usize,
    total_steps: usize,
    child_executions_seen: usize,

    // Invariant: stored in order sorted by leaf event index.
    high_priority: Vec<StepEvent<S>>,
    step_status: StepStatus<S>,
    // The last root event index that caused the data within this step to be
    // updated.
    last_root_event_index: RootEventIndex,
}

impl<S: StepSpec> EventBufferStepData<S> {
    fn new(
        step_info: StepInfo<NestedSpec>,
        parent_key_and_child_index: Option<(StepKey, usize)>,
        sort_key: StepSortKey,
        nest_level: usize,
        total_steps: usize,
        root_event_index: RootEventIndex,
    ) -> Self {
        Self {
            step_info,
            parent_key_and_child_index,
            sort_key,
            nest_level,
            total_steps,
            child_executions_seen: 0,
            high_priority: Vec::new(),
            step_status: StepStatus::NotStarted,
            last_root_event_index: root_event_index,
        }
    }

    #[inline]
    pub fn step_info(&self) -> &StepInfo<NestedSpec> {
        &self.step_info
    }

    #[inline]
    pub fn parent_key_and_child_index(&self) -> Option<(StepKey, usize)> {
        self.parent_key_and_child_index
    }

    #[inline]
    pub fn nest_level(&self) -> usize {
        self.nest_level
    }

    #[inline]
    pub fn total_steps(&self) -> usize {
        self.total_steps
    }

    #[inline]
    pub fn child_executions_seen(&self) -> usize {
        self.child_executions_seen
    }

    #[inline]
    pub fn step_status(&self) -> &StepStatus<S> {
        &self.step_status
    }

    #[inline]
    pub fn last_root_event_index(&self) -> RootEventIndex {
        self.last_root_event_index
    }

    #[inline]
    fn sort_key(&self) -> &StepSortKey {
        &self.sort_key
    }

    /// Returns step events since the provided event index.
    pub fn step_events_since(
        &self,
        last_seen: Option<usize>,
    ) -> Vec<&StepEvent<S>> {
        let mut events: Vec<_> =
            self.step_events_since_impl(last_seen).collect();
        events.sort_unstable_by_key(|event| event.event_index);
        events
    }

    // Returns step events since the provided event index.
    //
    // Does not necessarily return results in sorted order.
    fn step_events_since_impl(
        &self,
        last_seen: Option<usize>,
    ) -> impl Iterator<Item = &StepEvent<S>> {
        let iter = self
            .high_priority
            .iter()
            .filter(move |event| Some(event.event_index) > last_seen);
        let iter2 = self
            .step_status
            .low_priority()
            .filter(move |event| Some(event.event_index) > last_seen);
        iter.chain(iter2)
    }

    fn add_high_priority_step_event(&mut self, root_event: StepEvent<S>) {
        let root_event_index = RootEventIndex(root_event.event_index);
        // Dedup by the *leaf index* in case nested reports aren't deduped
        // coming in.
        match self.high_priority.binary_search_by(|probe| {
            probe.leaf_event_index().cmp(&root_event.leaf_event_index())
        }) {
            Ok(_) => {
                // This is a duplicate.
            }
            Err(index) => {
                // index is typically the last element, so this should be quite
                // efficient.
                self.update_root_event_index(root_event_index);
                self.high_priority.insert(index, root_event);
            }
        }
    }

    fn add_low_priority_step_event(
        &mut self,
        root_event: StepEvent<S>,
        max_low_priority: usize,
    ) {
        let root_event_index = RootEventIndex(root_event.event_index);
        let mut updated = false;
        match &mut self.step_status {
            StepStatus::NotStarted => {
                unreachable!(
                    "we always set progress before adding low-pri step events"
                );
            }
            StepStatus::Running { low_priority, .. } => {
                // Dedup by the *leaf index* in case nested reports aren't
                // deduped coming in.
                match low_priority.binary_search_by(|probe| {
                    probe.leaf_event_index().cmp(&root_event.leaf_event_index())
                }) {
                    Ok(_) => {
                        // This is a duplicate.
                    }
                    Err(index) => {
                        // The index is almost always at the end, so this is
                        // efficient enough.
                        low_priority.insert(index, root_event);
                        updated = true;
                    }
                }

                // Limit the number of events to the maximum low priority, ejecting
                // the oldest event(s) if necessary.
                while low_priority.len() > max_low_priority {
                    low_priority.pop_front();
                }
            }
            StepStatus::Completed { .. }
            | StepStatus::Failed { .. }
            | StepStatus::Aborted { .. }
            | StepStatus::WillNotBeRun { .. } => {
                // Ignore low-priority events for terminated steps since they're
                // likely duplicate events.
            }
        }

        if updated {
            self.update_root_event_index(root_event_index);
        }
    }

    fn mark_completed(
        &mut self,
        reason: CompletionReason,
        root_event_index: RootEventIndex,
    ) {
        match self.step_status {
            StepStatus::NotStarted | StepStatus::Running { .. } => {
                self.step_status = StepStatus::Completed { reason };
                self.update_root_event_index(root_event_index);
            }
            StepStatus::Completed { .. }
            | StepStatus::Failed { .. }
            | StepStatus::Aborted { .. }
            | StepStatus::WillNotBeRun { .. } => {
                // Ignore the status if the step has already been marked
                // terminated.
            }
        }
    }

    fn mark_failed(
        &mut self,
        reason: FailureReason,
        root_event_index: RootEventIndex,
    ) {
        match self.step_status {
            StepStatus::NotStarted | StepStatus::Running { .. } => {
                self.step_status = StepStatus::Failed { reason };
                self.update_root_event_index(root_event_index);
            }
            StepStatus::Completed { .. }
            | StepStatus::Failed { .. }
            | StepStatus::Aborted { .. }
            | StepStatus::WillNotBeRun { .. } => {
                // Ignore the status if the step has already been marked
                // terminated.
            }
        }
    }

    fn mark_aborted(
        &mut self,
        reason: AbortReason,
        root_event_index: RootEventIndex,
    ) {
        match &mut self.step_status {
            StepStatus::NotStarted => {
                match reason {
                    AbortReason::ParentAborted { parent_step, .. } => {
                        // A parent was aborted and this step hasn't been
                        // started.
                        self.step_status = StepStatus::WillNotBeRun {
                            reason: WillNotBeRunReason::ParentAborted {
                                step: parent_step,
                            },
                        };
                    }
                    AbortReason::StepAborted(info) => {
                        self.step_status = StepStatus::Aborted {
                            reason: AbortReason::StepAborted(info),
                            last_progress: None,
                        };
                    }
                }
                self.update_root_event_index(root_event_index);
            }
            StepStatus::Running { progress_event, .. } => {
                self.step_status = StepStatus::Aborted {
                    reason,
                    last_progress: Some(progress_event.clone()),
                };
                self.update_root_event_index(root_event_index);
            }
            StepStatus::Completed { .. }
            | StepStatus::Failed { .. }
            | StepStatus::Aborted { .. }
            | StepStatus::WillNotBeRun { .. } => {
                // Ignore the status if the step has already been marked
                // terminated.
            }
        }
    }

    fn mark_will_not_be_run(
        &mut self,
        reason: WillNotBeRunReason,
        root_event_index: RootEventIndex,
    ) {
        match self.step_status {
            StepStatus::NotStarted => {
                self.step_status = StepStatus::WillNotBeRun { reason };
                self.update_root_event_index(root_event_index);
            }
            StepStatus::Running { .. } => {
                // This is a weird situation. We should never encounter it in
                // normal use -- if we do encounter it, just ignore it.
            }
            StepStatus::Completed { .. }
            | StepStatus::Failed { .. }
            | StepStatus::Aborted { .. }
            | StepStatus::WillNotBeRun { .. } => {
                // Ignore the status if the step has already been marked
                // terminated.
            }
        }
    }

    fn set_progress(&mut self, current_progress: ProgressEvent<S>) {
        match &mut self.step_status {
            StepStatus::NotStarted => {
                self.step_status = StepStatus::Running {
                    low_priority: VecDeque::new(),
                    progress_event: current_progress,
                };
            }
            StepStatus::Running { progress_event, .. } => {
                *progress_event = current_progress;
            }
            StepStatus::Aborted { last_progress, .. } => {
                *last_progress = Some(current_progress);
            }
            StepStatus::Completed { .. }
            | StepStatus::Failed { .. }
            | StepStatus::WillNotBeRun { .. } => {
                // Ignore progress events for completed steps.
            }
        }
    }

    fn update_root_event_index(&mut self, root_event_index: RootEventIndex) {
        debug_assert!(
            root_event_index >= self.last_root_event_index,
            "event index must be monotonically increasing"
        );
        self.last_root_event_index =
            self.last_root_event_index.max(root_event_index);
    }
}

/// The step status as last seen by events.
#[derive_where(Clone, Debug)]
pub enum StepStatus<S: StepSpec> {
    NotStarted,

    /// The step is currently running.
    Running {
        // Invariant: stored in sorted order by index.
        low_priority: VecDeque<StepEvent<S>>,
        progress_event: ProgressEvent<S>,
    },

    /// The step has completed execution.
    Completed {
        /// The reason for completion.
        reason: CompletionReason,
    },

    /// The step has failed.
    Failed {
        /// The reason for the failure.
        reason: FailureReason,
    },

    /// Execution was aborted while this step was running.
    Aborted {
        /// The reason for the abort.
        reason: AbortReason,

        /// The last progress seen, if any.
        last_progress: Option<ProgressEvent<S>>,
    },

    /// The step will not be executed because a prior step failed.
    WillNotBeRun {
        /// The step that failed and caused this step to not be run.
        reason: WillNotBeRunReason,
    },
}

impl<S: StepSpec> StepStatus<S> {
    /// Returns true if this step is currently running.
    pub fn is_running(&self) -> bool {
        matches!(self, Self::Running { .. })
    }

    /// For failed steps, return the failure reason, otherwise None.
    pub fn failure_reason(&self) -> Option<&FailureReason> {
        match self {
            Self::Failed { reason, .. } => Some(reason),
            _ => None,
        }
    }

    /// For aborted steps, return the abort reason, otherwise None.
    pub fn abort_reason(&self) -> Option<&AbortReason> {
        // TODO: probably want to move last_progress into the `AbortReason`
        // enum so that we can return it in a reasonable manner here.
        match self {
            Self::Aborted { reason, .. } => Some(reason),
            _ => None,
        }
    }

    /// For will-not-be-run steps, return the reason, otherwise None.
    pub fn will_not_be_run_reason(&self) -> Option<&WillNotBeRunReason> {
        match self {
            Self::WillNotBeRun { reason } => Some(reason),
            _ => None,
        }
    }

    /// Returns low-priority events for this step, if any.
    ///
    /// Events are sorted by event index.
    pub fn low_priority(&self) -> impl Iterator<Item = &StepEvent<S>> {
        match self {
            Self::Running { low_priority, .. } => {
                Either::Left(low_priority.iter())
            }
            Self::NotStarted
            | Self::Completed { .. }
            | Self::Failed { .. }
            | Self::Aborted { .. }
            | Self::WillNotBeRun { .. } => Either::Right(std::iter::empty()),
        }
    }

    /// Returns the associated progress event for this step, if any.
    pub fn progress_event(&self) -> Option<&ProgressEvent<S>> {
        match self {
            Self::Running { progress_event, .. } => Some(progress_event),
            Self::Aborted { last_progress, .. } => last_progress.as_ref(),
            Self::NotStarted
            | Self::Completed { .. }
            | Self::Failed { .. }
            | Self::WillNotBeRun { .. } => None,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompletionReason {
    /// This step completed.
    StepCompleted(Arc<CompletionInfo>),
    /// A later step within the same execution was started and we don't have
    /// information regarding this step.
    SubsequentStarted {
        /// The later step that was started.
        later_step: StepKey,

        /// The root total elapsed time at the moment the later step was started.
        root_total_elapsed: Duration,
    },
    /// A parent step within the same execution completed and we don't have
    /// information regarding this step.
    ParentCompleted {
        /// The parent step that completed.
        parent_step: StepKey,

        /// Completion info associated with the parent step.
        parent_info: Arc<CompletionInfo>,
    },
}

impl CompletionReason {
    /// Returns the [`CompletionInfo`] for this step, if this is the
    /// [`Self::StepCompleted`] variant.
    pub fn step_completed_info(&self) -> Option<&Arc<CompletionInfo>> {
        match self {
            Self::StepCompleted(info) => Some(info),
            Self::SubsequentStarted { .. } | Self::ParentCompleted { .. } => {
                None
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct CompletionInfo {
    pub attempt: usize,
    pub outcome: StepOutcome<NestedSpec>,
    pub root_total_elapsed: Duration,
    pub leaf_total_elapsed: Duration,
    pub step_elapsed: Duration,
    pub attempt_elapsed: Duration,
}

#[derive(Clone, Debug)]
pub enum FailureReason {
    /// This step failed.
    StepFailed(Arc<FailureInfo>),
    /// A parent step failed.
    ParentFailed {
        /// The parent step that failed.
        parent_step: StepKey,

        /// Failure info associated with the parent step.
        parent_info: Arc<FailureInfo>,
    },
}

impl FailureReason {
    /// Returns the [`FailureInfo`] for this step, if this is the
    /// [`Self::StepFailed`] variant.
    pub fn step_failed_info(&self) -> Option<&Arc<FailureInfo>> {
        match self {
            Self::StepFailed(info) => Some(info),
            Self::ParentFailed { .. } => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct FailureInfo {
    pub total_attempts: usize,
    pub message: String,
    pub causes: Vec<String>,
    pub root_total_elapsed: Duration,
    pub leaf_total_elapsed: Duration,
    pub step_elapsed: Duration,
    pub attempt_elapsed: Duration,
}

#[derive(Clone, Debug)]
pub enum AbortReason {
    /// This step was aborted.
    StepAborted(Arc<AbortInfo>),
    /// A parent step was aborted.
    ParentAborted {
        /// The parent step key that was aborted.
        parent_step: StepKey,

        /// Abort info associated with the parent step.
        parent_info: Arc<AbortInfo>,
    },
}

impl AbortReason {
    /// Returns the [`AbortInfo`] for this step, if this is the
    /// [`Self::StepAborted`] variant.
    pub fn step_aborted_info(&self) -> Option<&Arc<AbortInfo>> {
        match self {
            Self::StepAborted(info) => Some(info),
            Self::ParentAborted { .. } => None,
        }
    }

    /// Returns a displayer for the message.
    ///
    /// The buffer is used to resolve step keys to step names.
    pub fn message_display<'a, S: StepSpec>(
        &'a self,
        buffer: &'a EventBuffer<S>,
    ) -> AbortMessageDisplay<'a, S> {
        AbortMessageDisplay::new(self, buffer)
    }
}

#[derive(Clone, Debug)]
pub enum WillNotBeRunReason {
    /// A preceding step failed.
    PreviousStepFailed {
        /// The previous step that failed.
        step: StepKey,
    },

    /// A parent step failed.
    ParentStepFailed {
        /// The parent step that failed.
        step: StepKey,
    },

    /// Execution was aborted during a previous step.
    PreviousStepAborted {
        /// The step which was aborted.
        step: StepKey,
    },

    /// A parent step was aborted.
    ParentAborted {
        /// The parent step which was aborted.
        step: StepKey,
    },
}

#[derive(Clone, Debug)]
pub struct AbortInfo {
    pub attempt: usize,
    pub message: String,

    /// The total elapsed time as reported by the root event.
    pub root_total_elapsed: Duration,

    /// The total elapsed time as reported by the leaf execution event, for
    /// nested events.
    pub leaf_total_elapsed: Duration,
    pub step_elapsed: Duration,
    pub attempt_elapsed: Duration,
}

#[derive(Clone, Debug)]
pub struct ExecutionSummary {
    pub total_steps: usize,
    pub execution_status: ExecutionStatus,
    // TODO: status about components
}

impl ExecutionSummary {
    // steps should be in order.
    fn new<S: StepSpec>(
        execution_id: ExecutionId,
        steps: &[&EventBufferStepData<S>],
    ) -> Self {
        let total_steps = steps.len();
        // Iterate through the steps to figure out the current status. Since
        // steps is in order, the last step that isn't NotStarted wins.
        let mut execution_status = ExecutionStatus::NotStarted;
        for data in steps {
            let step_key =
                StepKey { execution_id, index: data.step_info.index };
            match data.step_status() {
                StepStatus::NotStarted => {
                    // This step hasn't been started yet. Skip over it.
                }
                StepStatus::Running { low_priority, progress_event } => {
                    let root_total_elapsed = low_priority
                        .iter()
                        .map(|event| event.total_elapsed)
                        .chain(std::iter::once(progress_event.total_elapsed))
                        .max()
                        .expect("at least one value was provided");
                    execution_status = ExecutionStatus::Running {
                        step_key,
                        root_total_elapsed,
                    };
                }
                StepStatus::Completed { reason } => {
                    let (root_total_elapsed, leaf_total_elapsed) =
                        match reason.step_completed_info() {
                            Some(info) => (
                                Some(info.root_total_elapsed),
                                Some(info.leaf_total_elapsed),
                            ),
                            None => (None, None),
                        };

                    let terminal_status = ExecutionTerminalInfo {
                        kind: TerminalKind::Completed,
                        root_total_elapsed,
                        leaf_total_elapsed,
                        step_key,
                    };
                    execution_status =
                        ExecutionStatus::Terminal(terminal_status);
                }
                StepStatus::Failed { reason } => {
                    let (root_total_elapsed, leaf_total_elapsed) =
                        match reason.step_failed_info() {
                            Some(info) => (
                                Some(info.root_total_elapsed),
                                Some(info.leaf_total_elapsed),
                            ),
                            None => (None, None),
                        };

                    let terminal_status = ExecutionTerminalInfo {
                        kind: TerminalKind::Failed,
                        root_total_elapsed,
                        leaf_total_elapsed,
                        step_key,
                    };
                    execution_status =
                        ExecutionStatus::Terminal(terminal_status);
                }
                StepStatus::Aborted { reason, .. } => {
                    let (root_total_elapsed, leaf_total_elapsed) =
                        match reason.step_aborted_info() {
                            Some(info) => (
                                Some(info.root_total_elapsed),
                                Some(info.leaf_total_elapsed),
                            ),
                            None => (None, None),
                        };

                    let terminal_status = ExecutionTerminalInfo {
                        kind: TerminalKind::Aborted,
                        root_total_elapsed,
                        leaf_total_elapsed,
                        step_key,
                    };
                    execution_status =
                        ExecutionStatus::Terminal(terminal_status);
                }
                StepStatus::WillNotBeRun { .. } => {
                    // Ignore steps that will not be run -- a prior step failed.
                }
            };
        }

        Self { total_steps, execution_status }
    }
}

/// Step sort key.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub struct StepSortKey {
    // The tuples here are (defined at index, step index) pairs.
    values: Vec<(usize, usize)>,
}

impl StepSortKey {
    fn new(
        parent: Option<&Self>,
        defined_at_index: usize,
        step_index: usize,
    ) -> Self {
        let mut values = if let Some(parent) = parent {
            parent.values.clone()
        } else {
            Vec::new()
        };
        values.push((defined_at_index, step_index));
        Self { values }
    }
}

/// Status about a single execution ID.
///
/// Part of [`ExecutionSummary`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecutionStatus {
    /// This execution has not been started yet.
    NotStarted,

    /// This execution is currently running.
    Running {
        /// The step key that's currently running.
        ///
        /// Use [`EventBuffer::get`] to get more information about this step.
        step_key: StepKey,

        /// The maximum root_total_elapsed seen.
        root_total_elapsed: Duration,
    },

    /// Execution has finished.
    Terminal(ExecutionTerminalInfo),
}

/// Terminal status about a single execution ID.
///
/// Part of [`ExecutionStatus`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionTerminalInfo {
    /// The way in which this execution reached a terminal state.
    pub kind: TerminalKind,

    /// Total elapsed time (root) for this execution.
    ///
    /// The total elapsed time may not be available if execution was interrupted
    /// and we inferred that it was terminated.
    pub root_total_elapsed: Option<Duration>,

    /// Total elapsed time (leaf) for this execution.
    ///
    /// The total elapsed time may not be available if execution was interrupted
    /// and we inferred that it was terminated.
    pub leaf_total_elapsed: Option<Duration>,

    /// The step key that was running when this execution was terminated.
    ///
    /// * For completed executions, this is the last step that completed.
    /// * For failed or aborted executions, this is the step that failed.
    /// * For aborted executions, this is the step that was running when the
    ///   abort happened.
    pub step_key: StepKey,
}

/// The way in which an execution was terminated.
///
/// Part of [`ExecutionStatus`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TerminalKind {
    /// This execution completed running.
    Completed,
    /// This execution failed.
    Failed,
    /// This execution was aborted.
    Aborted,
}

impl fmt::Display for TerminalKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Completed => write!(f, "completed"),
            Self::Failed => write!(f, "failed"),
            Self::Aborted => write!(f, "aborted"),
        }
    }
}

impl ExecutionStatus {
    /// Returns the terminal status and the total amount of time elapsed, or
    /// None if the execution has not reached a terminal state.
    ///
    /// The time elapsed might be None if the execution was interrupted and
    /// completion information wasn't available.
    pub fn terminal_info(&self) -> Option<&ExecutionTerminalInfo> {
        match self {
            Self::NotStarted | Self::Running { .. } => None,
            Self::Terminal(info) => Some(info),
        }
    }
}

/// Keys for the event tree.
#[derive(Copy, Clone, Debug, Hash, Eq, PartialEq, Ord, PartialOrd)]
enum EventTreeNode {
    Root(ExecutionId),
    Step(StepKey),
}

/// A unique identifier for a group of step or progress events.
#[derive(Copy, Clone, Debug, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct StepKey {
    pub execution_id: ExecutionId,
    pub index: usize,
}

/// A newtype to track root event indexes within [`EventBuffer`]s, to ensure
/// that we aren't mixing them with leaf event indexes in this code.
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub struct RootEventIndex(pub usize);

impl fmt::Display for RootEventIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use anyhow::{bail, ensure, Context};
    use indexmap::IndexSet;
    use omicron_test_utils::dev::test_setup_log;
    use serde::{de::IntoDeserializer, Deserialize};

    use crate::{
        events::ProgressCounter,
        test_utils::{generate_test_events, GenerateTestEventsKind, TestSpec},
    };

    use super::*;

    #[tokio::test]
    async fn test_buffer() {
        let logctx = test_setup_log("test_buffer");
        let generated_events = generate_test_events(
            &logctx.log,
            GenerateTestEventsKind::Completed,
        )
        .await;

        let test_cx = BufferTestContext::new(generated_events);

        test_cx
            .run_all_elements_test(
                "all events passed in one-by-one",
                |buffer, event| buffer.add_event(event.clone()),
                1,
            )
            .unwrap();

        test_cx
            .run_all_elements_test(
                "all events duplicated (1)",
                |buffer, event| {
                    buffer.add_event(event.clone());
                    buffer.add_event(event.clone());
                },
                1,
            )
            .unwrap();

        test_cx
            .run_all_elements_test(
                "all events duplicated (2)",
                |buffer, event| {
                    buffer.add_event(event.clone());
                },
                2,
            )
            .unwrap();

        test_cx
            .run_filtered_test(
                "all events passed in",
                |buffer, event| {
                    buffer.add_event(event.clone());
                    true
                },
                WithDeltas::No,
            )
            .unwrap();

        test_cx
            .run_filtered_test(
                "progress events skipped",
                |buffer, event| match event {
                    Event::Step(event) => {
                        buffer.add_step_event(event.clone());
                        true
                    }
                    Event::Progress(_) => false,
                },
                WithDeltas::Both,
            )
            .unwrap();

        test_cx
            .run_filtered_test(
                "low-priority events skipped",
                |buffer, event| match event {
                    Event::Step(event) => match event.kind.priority() {
                        StepEventPriority::High => {
                            buffer.add_step_event(event.clone());
                            true
                        }
                        StepEventPriority::Low => false,
                    },
                    Event::Progress(event) => {
                        buffer.add_progress_event(event.clone());
                        true
                    }
                },
                WithDeltas::Both,
            )
            .unwrap();

        test_cx
            .run_filtered_test(
                "low-priority and progress events skipped",
                |buffer, event| match event {
                    Event::Step(event) => match event.kind.priority() {
                        StepEventPriority::High => {
                            buffer.add_step_event(event.clone());
                            true
                        }
                        StepEventPriority::Low => false,
                    },
                    Event::Progress(_) => {
                        // Don't add progress events.
                        false
                    }
                },
                WithDeltas::Both,
            )
            .unwrap();

        logctx.cleanup_successful();
    }

    /// This number is small enough that it will cause low-priority events to be
    /// dropped in some cases.
    const MAX_LOW_PRIORITY: usize = 4;

    #[derive(Debug)]
    struct BufferTestContext {
        root_execution_id: ExecutionId,
        generated_events: Vec<Event<TestSpec>>,

        // Data derived from generated_events.
        generated_step_events: Vec<StepEvent<TestSpec>>,
    }

    impl BufferTestContext {
        fn new(generated_events: Vec<Event<TestSpec>>) -> Self {
            // The first event is always a root event.
            let root_execution_id =
                match generated_events.first().expect("at least one event") {
                    Event::Step(event) => event.execution_id,
                    Event::Progress(_) => {
                        panic!("first event should always be a step event")
                    }
                };

            // Ensure that nested step 2 produces progress events in the
            // expected order and in succession.
            let mut progress_check = NestedProgressCheck::new();
            for event in &generated_events {
                if let Event::Progress(event) = event {
                    let progress_counter = event.kind.progress_counter();
                    if progress_counter
                        == Some(&ProgressCounter::new(2, 3, "steps"))
                    {
                        progress_check.two_out_of_three_seen();
                    } else if progress_check
                        == NestedProgressCheck::TwoOutOfThreeSteps
                    {
                        assert_eq!(
                            progress_counter,
                            Some(&ProgressCounter::current(50, "units"))
                        );
                        progress_check.fifty_units_seen();
                    } else if progress_check == NestedProgressCheck::FiftyUnits
                    {
                        assert_eq!(
                            progress_counter,
                            Some(&ProgressCounter::new(3, 3, "steps"))
                        );
                        progress_check.three_out_of_three_seen();
                    }
                }
            }
            progress_check.assert_done();

            // Ensure that events are never seen twice.
            let mut event_indexes_seen = HashSet::new();
            let mut leaf_event_indexes_seen = HashSet::new();
            let generated_step_events: Vec<_> = generated_events
                .iter()
                .filter_map(|event| match event {
                    Event::Step(event) => {
                        if event_indexes_seen.contains(&event.event_index) {
                            panic!(
                                "duplicate event seen for index {}",
                                event.event_index
                            );
                        }
                        event_indexes_seen.insert(event.event_index);

                        let leaf_event_key = (
                            event.leaf_execution_id(),
                            event.leaf_event_index(),
                        );
                        if leaf_event_indexes_seen.contains(&leaf_event_key) {
                            panic!(
                                "duplicate leaf event seen \
                                 for key {leaf_event_key:?}",
                            );
                        }
                        leaf_event_indexes_seen.insert(leaf_event_key);

                        Some(event.clone())
                    }
                    Event::Progress(_) => None,
                })
                .collect();

            // Create two buffer and feed events.
            // * The incremental buffer has each event fed into it one-by-one.
            // * The "idempotent" buffer has events 0, 0..1, 0..2, 0..3, etc
            //   fed into it one by one. The name is because this is really
            //   testing the idempotency of the event buffer.

            println!("** generating incremental and idempotent buffers **");
            let mut incremental_buffer = EventBuffer::default();
            let mut idempotent_buffer = EventBuffer::default();
            for event in &generated_events {
                incremental_buffer.add_event(event.clone());
                let report = incremental_buffer.generate_report();
                idempotent_buffer.add_event_report(report);
            }

            // Check that the two buffers above are similar.
            Self::ensure_buffers_similar(
                &incremental_buffer,
                &idempotent_buffer,
            )
            .expect("idempotent buffer is similar to incremental buffer");

            // Also generate a buffer with a single event report.
            println!("** generating oneshot buffer **");
            let mut oneshot_buffer = EventBuffer::default();
            oneshot_buffer
                .add_event_report(incremental_buffer.generate_report());

            Self::ensure_buffers_similar(&incremental_buffer, &oneshot_buffer)
                .expect("oneshot buffer is similar to incremental buffer");

            Self { root_execution_id, generated_events, generated_step_events }
        }

        fn ensure_buffers_similar<S: StepSpec>(
            buf1: &EventBuffer<S>,
            buf2: &EventBuffer<S>,
        ) -> anyhow::Result<()> {
            // The two should have the same step keys.
            let buf1_steps = buf1.steps();
            let buf2_steps = buf2.steps();

            ensure!(
                buf1_steps.as_slice().len() == buf2_steps.as_slice().len(),
                "buffers have same number of steps ({} vs {})",
                buf1_steps.as_slice().len(),
                buf2_steps.as_slice().len()
            );

            for (ix, ((k1, data1), (k2, data2))) in buf1_steps
                .as_slice()
                .iter()
                .zip(buf2_steps.as_slice().iter())
                .enumerate()
            {
                ensure!(
                    k1 == k2,
                    "buffers have same step keys at index {} ({:?} vs {:?})",
                    ix,
                    k1,
                    k2
                );
                ensure!(
                    data1.sort_key() == data2.sort_key(),
                    "buffers have same sort key at index {} ({:?} vs {:?})",
                    ix,
                    data1.sort_key(),
                    data2.sort_key()
                );
                ensure!(
                    data1.parent_key_and_child_index() == data2.parent_key_and_child_index(),
                    "buffers have same parent key and child index at index {} ({:?} vs {:?})",
                    ix,
                    data1.parent_key_and_child_index(),
                    data2.parent_key_and_child_index(),
                );
                ensure!(
                    data1.nest_level() == data2.nest_level(),
                    "buffers have same nest level at index {} ({:?} vs {:?})",
                    ix,
                    data1.nest_level(),
                    data2.nest_level(),
                );
            }

            Ok(())
        }

        /// Runs a test in a scenario where all elements should be seen.
        ///
        /// Each event is added `times` times.
        fn run_all_elements_test(
            &self,
            description: &str,
            mut event_fn: impl FnMut(&mut EventBuffer<TestSpec>, &Event<TestSpec>),
            times: usize,
        ) -> anyhow::Result<()> {
            let mut buffer: EventBuffer<TestSpec> =
                EventBuffer::new(MAX_LOW_PRIORITY);
            let mut reported_step_events = Vec::new();
            let mut last_seen = None;

            for (i, event) in self.generated_events.iter().enumerate() {
                for time in 0..times {
                    (event_fn)(&mut buffer, event);
                    let report = buffer.generate_report_since(&mut last_seen);
                    let is_last_event = i == self.generated_events.len() - 1;
                    self.assert_general_properties(
                        &buffer,
                        &report,
                        is_last_event,
                    )
                    .with_context(|| {
                        format!(
                            "{description}, at index {i} (time {time}), \
                                properties not met"
                        )
                    })
                    .unwrap();
                    reported_step_events.extend(report.step_events);

                    // Ensure that the last root index was updated for this
                    // event's corresponding steps, but not for any others.
                    if let Event::Step(event) = event {
                        check_last_root_event_index(event, &buffer)
                            .with_context(|| {
                                format!(
                                    "{description}, at index {i} (time {time}):\
                                     error with last root event index"
                                )
                            })?;
                    }

                    // Call last_seen without feeding a new event in to ensure that
                    // a report with no step events is produced.
                    let mut last_seen_2 = last_seen;
                    let report = buffer.generate_report_since(&mut last_seen_2);
                    ensure!(
                        report.step_events.is_empty(),
                        "{description}, at index {i} (time {time}),\
                        no step events are seen"
                    );
                    ensure!(
                        report.last_seen == last_seen,
                        "{description}, at index {i} (time {time}), \
                        report.last_seen {:?} matches last_seen {:?}",
                        report.last_seen,
                        last_seen,
                    );
                }
            }

            ensure!(
                self.generated_step_events == reported_step_events,
                "all generated step events were reported"
            );

            Ok(())
        }

        /// Runs a test in a scenario where not all events might be replayed.
        fn run_filtered_test(
            &self,
            event_fn_description: &str,
            mut event_fn: impl FnMut(
                &mut EventBuffer<TestSpec>,
                &Event<TestSpec>,
            ) -> bool,
            with_deltas: WithDeltas,
        ) -> anyhow::Result<()> {
            match with_deltas {
                WithDeltas::Yes => {
                    self.run_filtered_test_inner(&mut event_fn, true)
                        .context(event_fn_description.to_owned())?;
                }
                WithDeltas::No => {
                    self.run_filtered_test_inner(&mut event_fn, false)
                        .context(event_fn_description.to_owned())?;
                }
                WithDeltas::Both => {
                    self.run_filtered_test_inner(&mut event_fn, true)
                        .context(event_fn_description.to_owned())?;
                    self.run_filtered_test_inner(&mut event_fn, false)
                        .context(event_fn_description.to_owned())?;
                }
            }

            Ok(())
        }

        fn run_filtered_test_inner(
            &self,
            mut event_fn: impl FnMut(
                &mut EventBuffer<TestSpec>,
                &Event<TestSpec>,
            ) -> bool,
            with_deltas: bool,
        ) -> anyhow::Result<()> {
            let description = format!("with deltas = {with_deltas}");
            let mut buffer = EventBuffer::new(MAX_LOW_PRIORITY);
            let mut last_high_priority = Vec::new();

            // This buffer has a large enough capacity that it never drops
            // events.
            let mut receive_buffer = EventBuffer::new(1024);
            // last_seen_opt is None if with_deltas is false.
            //
            // Some(None) if with_deltas is true and no events have been seen so far.
            //
            // Some(Some(index)) if with_deltas is true and events have been seen.
            let mut last_seen_opt = with_deltas.then_some(None);

            for (i, event) in self.generated_events.iter().enumerate() {
                let event_added = (event_fn)(&mut buffer, event);

                let report = match &mut last_seen_opt {
                    Some(last_seen) => buffer.generate_report_since(last_seen),
                    None => buffer.generate_report(),
                };

                let is_last_event = i == self.generated_events.len() - 1;
                self.assert_general_properties(&buffer, &report, is_last_event)
                    .with_context(|| {
                        format!(
                            "{description}, at index {i}, properties not met"
                        )
                    })
                    .unwrap();

                if let Event::Step(event) = event {
                    if event_added {
                        check_last_root_event_index(event, &buffer)
                            .with_context(|| {
                                format!(
                                    "{description}, at index {i}: \
                                    error with last root event index"
                                )
                            })?;
                    }
                }

                receive_buffer.add_event_report(report.clone());
                let this_step_events =
                    receive_buffer.generate_report().step_events;
                let this_high_priority: Vec<_> = this_step_events
                    .iter()
                    .filter(|event| {
                        event.kind.priority() == StepEventPriority::High
                    })
                    .cloned()
                    .collect();

                if !with_deltas {
                    let report_high_priority: Vec<_> = report
                        .step_events
                        .iter()
                        .filter(|event| {
                            event.kind.priority() == StepEventPriority::High
                        })
                        .cloned()
                        .collect();
                    ensure!(
                        this_high_priority == report_high_priority,
                        "{description}, at index {i}, \
                         all high-priority events reported"
                    );
                }

                if this_high_priority.len() == last_high_priority.len() {
                    // event is not a high-priority event. All old high-priority
                    // events must be reported as well.
                    ensure!(
                        this_high_priority == last_high_priority,
                        "{description}, at index {i}, \
                         all old high-priority events reported (1)"
                    );
                } else if this_high_priority.len()
                    == last_high_priority.len() + 1
                {
                    // The first N events must match.
                    ensure!(
                        &this_high_priority[0..last_high_priority.len()]
                            == &last_high_priority,
                        "{description}, at index {i}, \
                         all old high-priority events reported (2)"
                    );
                }

                last_high_priority = this_high_priority;
            }

            Ok(())
        }

        fn assert_general_properties<S: StepSpec>(
            &self,
            buffer: &EventBuffer<TestSpec>,
            report: &EventReport<S>,
            is_last_event: bool,
        ) -> anyhow::Result<()> {
            let mut progress_keys_seen = HashSet::new();
            for event in &report.progress_events {
                let key = progress_event_key(event);
                // Ensure that there's just one progress event per report.
                if progress_keys_seen.contains(&key) {
                    bail!("progress event key {key:?} seen twice in event map")
                }
                progress_keys_seen.insert(key);
                // Check that the buffer has an event in the tree corresponding
                // to any reports seen.
                if !buffer
                    .event_store
                    .event_tree
                    .contains_node(EventTreeNode::Step(key))
                {
                    bail!("progress event key {key:?} not found in event tree");
                }
                if !buffer.event_store.map.contains_key(&key) {
                    bail!("progress event key {key:?} not found in event map");
                }
            }

            // Assert that steps are always in order. To check this, we use step
            // IDs, which we externally define to be in order.
            let steps = buffer.steps();
            for window in steps.as_slice().windows(2) {
                let (_, data1) = window[0];
                let (_, data2) = window[1];
                let data1_id: usize = Deserialize::deserialize(
                    data1.step_info().id.clone().into_deserializer(),
                )
                .expect("data1.id is a usize");
                let data2_id: usize = Deserialize::deserialize(
                    data2.step_info().id.clone().into_deserializer(),
                )
                .expect("data2.id is a usize");
                ensure!(
                    data1_id < data2_id,
                    "data 1 ID {data1_id} < data 2 ID {data2_id}"
                );
            }

            // The root execution ID should have a summary associated with it.
            let root_execution_id = buffer
                .root_execution_id()
                .expect("at least one event => root execution ID exists");
            ensure!(
                root_execution_id == self.root_execution_id,
                "root execution ID matches"
            );
            let summary = steps.summarize();
            ensure!(
                summary.contains_key(&root_execution_id),
                "summary contains root execution ID {root_execution_id:?}"
            );

            if is_last_event {
                ensure!(
                    matches!(
                        &summary[&root_execution_id].execution_status,
                        ExecutionStatus::Terminal(info)
                            if info.kind == TerminalKind::Completed
                    ),
                    "this is the last event so ExecutionStatus must be completed"
                );
                // There are three nested engines.
                ensure!(
                    summary.len() == 4,
                    "three nested engines (plus one root engine) must be defined"
                );

                let (_, nested_summary) = summary
                    .get_index(1)
                    .expect("this is the first nested engine");
                ensure!(
                    matches!(
                        &nested_summary.execution_status,
                        ExecutionStatus::Terminal(info)
                            if info.kind == TerminalKind::Failed
                    ),
                    "for this engine, the ExecutionStatus must be failed"
                );

                let (_, nested_summary) = summary
                    .get_index(2)
                    .expect("this is the second nested engine");
                ensure!(
                    matches!(
                        &nested_summary.execution_status,
                        ExecutionStatus::Terminal(info)
                            if info.kind == TerminalKind::Failed
                    ),
                    "for this engine, the ExecutionStatus must be failed"
                );

                let (_, nested_summary) = summary
                    .get_index(3)
                    .expect("this is the third nested engine");
                ensure!(
                    matches!(
                        &nested_summary.execution_status,
                        ExecutionStatus::Terminal(info)
                            if info.kind == TerminalKind::Completed
                    ),
                    "for this engine, the ExecutionStatus must be succeeded"
                );
            } else {
                ensure!(
                    matches!(
                        summary[&root_execution_id].execution_status,
                        ExecutionStatus::Running { .. },
                    ),
                    "not the last event so ExecutionStatus must be running"
                );
            }

            // The root execution ID should be the only root in the event tree.
            for node in buffer.event_store.event_tree.nodes() {
                let count = buffer
                    .event_store
                    .event_tree
                    .neighbors_directed(node, Direction::Incoming)
                    .count();
                if node == EventTreeNode::Root(root_execution_id) {
                    ensure!(
                        count == 0,
                        "for root execution ID, \
                        incoming neighbors should be 0"
                    );
                } else {
                    ensure!(
                        count > 0,
                        "for non-root execution ID, \
                         incoming neighbors should be > 0"
                    );
                }
            }

            Ok(())
        }
    }

    fn check_last_root_event_index(
        event: &StepEvent<TestSpec>,
        buffer: &EventBuffer<TestSpec>,
    ) -> anyhow::Result<()> {
        let root_event_index = RootEventIndex(event.event_index);
        let event_step_keys = step_keys(event);
        let steps = buffer.steps();
        for (step_key, data) in steps.as_slice() {
            let data_index = data.last_root_event_index();
            if event_step_keys.contains(step_key) {
                ensure!(
                    data_index == root_event_index,
                    "last_root_event_index should have been updated \
                     but wasn't (actual: {data_index}, expected: {root_event_index}) \
                     for step {step_key:?} (event: {event:?})",
                );
            } else {
                ensure!(
                    data_index < root_event_index,
                    "last_root_event_index should *not* have been updated \
                     but was (current: {data_index}, new: {root_event_index}) \
                     for step {step_key:?} (event: {event:?})",
                );
            }
        }

        Ok(())
    }

    /// Returns the step keys that this step event would cause updates against,
    /// in order from root to leaf.
    fn step_keys<S: StepSpec>(event: &StepEvent<S>) -> IndexSet<StepKey> {
        let mut out = IndexSet::new();
        step_keys_impl(event, &mut out);
        out
    }

    fn step_keys_impl<S: StepSpec>(
        event: &StepEvent<S>,
        out: &mut IndexSet<StepKey>,
    ) {
        match &event.kind {
            StepEventKind::NoStepsDefined | StepEventKind::Unknown => {}
            StepEventKind::ExecutionStarted { steps, .. } => {
                for step in steps {
                    out.insert(StepKey {
                        execution_id: event.execution_id,
                        index: step.index,
                    });
                }
            }
            StepEventKind::ProgressReset { step, .. }
            | StepEventKind::AttemptRetry { step, .. }
            | StepEventKind::StepCompleted { step, .. }
            | StepEventKind::ExecutionCompleted { last_step: step, .. }
            | StepEventKind::ExecutionFailed { failed_step: step, .. }
            | StepEventKind::ExecutionAborted { aborted_step: step, .. } => {
                out.insert(StepKey {
                    execution_id: event.execution_id,
                    index: step.info.index,
                });
            }
            StepEventKind::Nested { step, event, .. } => {
                out.insert(StepKey {
                    execution_id: event.execution_id,
                    index: step.info.index,
                });
                step_keys_impl(event, out);
            }
        }
    }

    #[derive(Copy, Clone, Debug)]
    #[allow(unused)]
    enum WithDeltas {
        Yes,
        No,
        Both,
    }

    fn progress_event_key<S: StepSpec>(event: &ProgressEvent<S>) -> StepKey {
        match &event.kind {
            ProgressEventKind::WaitingForProgress { step, .. }
            | ProgressEventKind::Progress { step, .. } => StepKey {
                execution_id: event.execution_id,
                index: step.info.index,
            },
            ProgressEventKind::Nested { event: nested_event, .. } => {
                progress_event_key(nested_event)
            }
            ProgressEventKind::Unknown => {
                panic!("we should never generate an unknown key")
            }
        }
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum NestedProgressCheck {
        Initial,
        TwoOutOfThreeSteps,
        FiftyUnits,
        ThreeOutOfThreeSteps,
    }

    impl NestedProgressCheck {
        fn new() -> Self {
            Self::Initial
        }

        fn two_out_of_three_seen(&mut self) {
            assert_eq!(
                *self,
                Self::Initial,
                "two_out_of_three_seen: expected Initial",
            );
            *self = Self::TwoOutOfThreeSteps;
        }

        fn fifty_units_seen(&mut self) {
            assert_eq!(
                *self,
                Self::TwoOutOfThreeSteps,
                "fifty_units_seen: expected TwoOutOfThreeSteps",
            );
            *self = Self::FiftyUnits;
        }

        fn three_out_of_three_seen(&mut self) {
            assert_eq!(
                *self,
                Self::FiftyUnits,
                "three_out_of_three_seen: expected FiftyUnits",
            );
            *self = Self::ThreeOutOfThreeSteps;
        }

        fn assert_done(&self) {
            assert_eq!(
                *self,
                Self::ThreeOutOfThreeSteps,
                "assert_done: expected ThreeOutOfThreeSteps",
            );
        }
    }
}
