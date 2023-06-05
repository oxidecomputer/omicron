// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use std::{
    collections::{HashMap, VecDeque},
    time::Duration,
};

use derive_where::derive_where;
use either::Either;
use indexmap::IndexMap;
use petgraph::{prelude::*, visit::Walker};

use crate::{
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
        self.event_store.handle_step_event(event, self.max_low_priority);
    }

    /// Returns the root execution ID, if this event buffer is aware of any
    /// events.
    pub fn root_execution_id(&self) -> Option<ExecutionId> {
        self.event_store.root_execution_id
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
        self.generate_report_since(None)
    }

    pub fn generate_report_since(
        &self,
        mut last_seen: Option<usize>,
    ) -> EventReport<S> {
        // Gather step events across all keys.
        let mut step_events = Vec::new();
        let mut progress_events = Vec::new();
        for (_, step_data) in self.steps().as_slice() {
            step_events.extend(step_data.step_events_since(last_seen).cloned());
            progress_events
                .extend(step_data.step_status.progress_event().cloned());
        }

        // Sort events.
        step_events.sort_unstable_by_key(|event| event.event_index);
        progress_events.sort_unstable_by_key(|event| event.total_elapsed);
        if let Some(last) = step_events.last() {
            // Only update last_seen if there are new step events (otherwise it
            // stays the same).
            last_seen = Some(last.event_index);
        }

        EventReport {
            step_events,
            progress_events,
            root_execution_id: self.root_execution_id(),
            last_seen,
        }
    }

    /// Returns true if any further step events are pending since `last_seen`.
    ///
    /// This does not currently care about pending progress events, just pending
    /// step events. A typical use for this is to check that all step events
    /// have been reported before a sender shuts down.
    pub fn has_pending_events_since(&self, last_seen: Option<usize>) -> bool {
        for (_, step_data) in self.steps().as_slice() {
            if step_data.step_events_since(last_seen).next().is_some() {
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

    /// Handles a step event.
    fn handle_step_event(
        &mut self,
        event: StepEvent<S>,
        max_low_priority: usize,
    ) {
        if matches!(event.kind, StepEventKind::Unknown) {
            // Ignore unknown events.
            return;
        }

        let actions =
            self.recurse_for_step_event(&event, 0, None, event.event_index);
        if let Some(new_execution) = actions.new_execution {
            if new_execution.nest_level == 0 {
                self.root_execution_id = Some(new_execution.execution_id);
            }
            for (new_step_key, new_step, sort_key) in new_execution.steps_to_add
            {
                // These are brand new steps so their keys shouldn't exist in the
                // map. But if they do, don't overwrite them.
                self.map.entry(new_step_key).or_insert_with(|| {
                    EventBufferStepData::new(
                        new_step,
                        sort_key,
                        new_execution.nest_level,
                    )
                });
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
        parent_sort_key: Option<&StepSortKey>,
        root_event_index: usize,
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
                        root_event_index,
                        step.index,
                    );
                    let step_node = self.add_step_node(step_key);
                    self.event_tree.add_edge(root_node, step_node, ());
                    let step_info = step.clone().into_generic();
                    steps_to_add.push((step_key, step_info, sort_key));
                }
                new_execution = Some(NewExecutionAction {
                    execution_id: event.execution_id,
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
                    step_elapsed: *step_elapsed,
                    attempt_elapsed: *attempt_elapsed,
                };
                // Mark this key and all child keys completed.
                self.mark_step_key_completed(key, info);

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
                    step_elapsed: *step_elapsed,
                    attempt_elapsed: *attempt_elapsed,
                };
                // Mark this key and all child keys completed.
                self.mark_execution_id_completed(key, info);

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
                    step_elapsed: *step_elapsed,
                    attempt_elapsed: *attempt_elapsed,
                };
                self.mark_step_failed(key, info);

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
                    step_elapsed: *step_elapsed,
                    attempt_elapsed: *attempt_elapsed,
                };
                self.mark_step_aborted(key, info);

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
                    parent_sort_key.as_ref(),
                    root_event_index,
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
    ) {
        if let Some(value) = self.map.get_mut(&root_key) {
            // Completion status only applies to the root key. Nodes reachable
            // from this node are still marked as complete, but without status.
            value.mark_completed(Some(info));
        }

        // Mark anything reachable from this node as completed.
        let mut dfs =
            DfsPostOrder::new(&self.event_tree, EventTreeNode::Step(root_key));
        while let Some(key) = dfs.next(&self.event_tree) {
            if let EventTreeNode::Step(key) = key {
                if key != root_key {
                    if let Some(value) = self.map.get_mut(&key) {
                        value.mark_completed(None);
                    }
                }
            }
        }
    }

    fn mark_execution_id_completed(
        &mut self,
        root_key: StepKey,
        info: CompletionInfo,
    ) {
        if let Some(value) = self.map.get_mut(&root_key) {
            // Completion status only applies to the root key.
            value.mark_completed(Some(info));
        }

        let mut dfs = DfsPostOrder::new(
            &self.event_tree,
            EventTreeNode::Root(root_key.execution_id),
        );
        while let Some(key) = dfs.next(&self.event_tree) {
            if let EventTreeNode::Step(key) = key {
                if key != root_key {
                    if let Some(value) = self.map.get_mut(&key) {
                        value.mark_completed(None);
                    }
                }
            }
        }
    }

    fn mark_step_failed(&mut self, root_key: StepKey, info: FailureInfo) {
        self.mark_step_failed_impl(root_key, |value, kind| {
            match kind {
                MarkStepFailedImplKind::Root => {
                    value.mark_failed(Some(info.clone()));
                }
                MarkStepFailedImplKind::Descendant => {
                    value.mark_failed(None);
                }
                MarkStepFailedImplKind::Future => {
                    value.mark_will_not_be_run(
                        WillNotBeRunReason::PreviousStepFailed {
                            step: root_key,
                        },
                    );
                }
            };
        })
    }

    fn mark_step_aborted(&mut self, root_key: StepKey, info: AbortInfo) {
        self.mark_step_failed_impl(root_key, |value, kind| {
            match kind {
                MarkStepFailedImplKind::Root => {
                    value.mark_aborted(AbortReason::StepAborted(info.clone()));
                }
                MarkStepFailedImplKind::Descendant => {
                    value.mark_aborted(AbortReason::ParentAborted {
                        parent_step: root_key,
                    });
                }
                MarkStepFailedImplKind::Future => {
                    value.mark_will_not_be_run(
                        WillNotBeRunReason::PreviousStepAborted {
                            step: root_key,
                        },
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
                value.mark_completed(None);
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
                    (cb)(value, MarkStepFailedImplKind::Future);
                }
            }
        }
    }
}

enum MarkStepFailedImplKind {
    Root,
    Descendant,
    Future,
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
    nest_level: usize,
    // Invariant: stored in order sorted by event_index.
    high_priority: Vec<StepEvent<S>>,
    step_status: StepStatus<S>,
}

impl<S: StepSpec> EventBufferStepData<S> {
    fn new(
        step_info: StepInfo<NestedSpec>,
        sort_key: StepSortKey,
        nest_level: usize,
    ) -> Self {
        Self {
            step_info,
            sort_key,
            nest_level,
            high_priority: Vec::new(),
            step_status: StepStatus::NotStarted,
        }
    }

    pub fn step_info(&self) -> &StepInfo<NestedSpec> {
        &self.step_info
    }

    pub fn nest_level(&self) -> usize {
        self.nest_level
    }

    pub fn step_status(&self) -> &StepStatus<S> {
        &self.step_status
    }

    fn sort_key(&self) -> &StepSortKey {
        &self.sort_key
    }

    // Returns step events since the provided event index.
    //
    // Does not necessarily return results in sorted order.
    fn step_events_since(
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

    fn add_high_priority_step_event(&mut self, event: StepEvent<S>) {
        // Dedup by the *leaf index* in case nested reports aren't deduped
        // coming in.
        match self.high_priority.binary_search_by(|probe| {
            probe.leaf_event_index().cmp(&event.leaf_event_index())
        }) {
            Ok(_) => {
                // This is a duplicate.
            }
            Err(index) => {
                // index is typically the last element, so this should be quite
                // efficient.
                self.high_priority.insert(index, event);
            }
        }
    }

    fn add_low_priority_step_event(
        &mut self,
        event: StepEvent<S>,
        max_low_priority: usize,
    ) {
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
                    probe.leaf_event_index().cmp(&event.leaf_event_index())
                }) {
                    Ok(_) => {
                        // This is a duplicate.
                    }
                    Err(index) => {
                        // The index is almost always at the end, so this is
                        // efficient enough.
                        low_priority.insert(index, event);
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
    }

    fn mark_completed(&mut self, status: Option<CompletionInfo>) {
        match self.step_status {
            StepStatus::NotStarted | StepStatus::Running { .. } => {
                self.step_status = StepStatus::Completed { info: status };
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

    fn mark_failed(&mut self, info: Option<FailureInfo>) {
        match self.step_status {
            StepStatus::NotStarted | StepStatus::Running { .. } => {
                self.step_status = StepStatus::Failed { info };
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

    fn mark_aborted(&mut self, reason: AbortReason) {
        match &mut self.step_status {
            StepStatus::NotStarted => {
                match reason {
                    AbortReason::ParentAborted { parent_step } => {
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
            }
            StepStatus::Running { progress_event, .. } => {
                self.step_status = StepStatus::Aborted {
                    reason,
                    last_progress: Some(progress_event.clone()),
                };
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

    fn mark_will_not_be_run(&mut self, reason: WillNotBeRunReason) {
        match self.step_status {
            StepStatus::NotStarted => {
                self.step_status = StepStatus::WillNotBeRun { reason };
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
        /// Completion information.
        ///
        /// This might be unavailable in some cases.
        info: Option<CompletionInfo>,
    },

    /// The step has failed.
    Failed {
        /// Failure information.
        info: Option<FailureInfo>,
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
pub struct CompletionInfo {
    pub attempt: usize,
    pub outcome: StepOutcome<NestedSpec>,
    pub step_elapsed: Duration,
    pub attempt_elapsed: Duration,
}

#[derive(Clone, Debug)]
pub struct FailureInfo {
    pub total_attempts: usize,
    pub message: String,
    pub causes: Vec<String>,
    pub step_elapsed: Duration,
    pub attempt_elapsed: Duration,
}

#[derive(Clone, Debug)]
pub enum AbortReason {
    /// This step was aborted.
    StepAborted(AbortInfo),
    /// A parent step was aborted.
    ParentAborted {
        /// The parent step key that was aborted.
        parent_step: StepKey,
    },
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
                StepStatus::Running { .. } => {
                    execution_status = ExecutionStatus::Running { step_key };
                }
                StepStatus::Completed { .. } => {
                    execution_status = ExecutionStatus::Completed { step_key };
                }
                StepStatus::Failed { .. } => {
                    execution_status = ExecutionStatus::Failed { step_key };
                }
                StepStatus::Aborted { .. } => {
                    execution_status = ExecutionStatus::Aborted { step_key };
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
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionStatus {
    /// This execution has not been started yet.
    NotStarted,

    /// This execution is currently running.
    Running {
        /// The step key that's currently running.
        ///
        /// Use [`EventBuffer::get`] to get more information about this step.
        step_key: StepKey,
    },

    /// This execution completed running.
    Completed {
        /// The last step that completed.
        step_key: StepKey,
    },

    /// This execution failed.
    Failed {
        /// The step key that failed.
        ///
        /// Use [`EventBuffer::get`] to get more information about this step.
        step_key: StepKey,
    },

    /// This execution was aborted.
    Aborted {
        /// The step that was running when the abort happened.
        ///
        /// Use [`EventBuffer::get`] to get more information about this step.
        step_key: StepKey,
    },
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

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use anyhow::{bail, ensure, Context};
    use futures::StreamExt;
    use omicron_test_utils::dev::test_setup_log;
    use serde::{de::IntoDeserializer, Deserialize};
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;

    use crate::{
        events::StepProgress, test_utils::TestSpec, StepContext, StepSuccess,
        UpdateEngine,
    };

    use super::*;

    #[tokio::test]
    async fn test_buffer() {
        let logctx = test_setup_log("test_buffer");
        // The channel is big enough to contain all possible events.
        let (sender, receiver) = mpsc::channel(512);
        let engine: UpdateEngine<TestSpec> =
            UpdateEngine::new(&logctx.log, sender);

        engine
            .new_step("foo".to_owned(), 1, "Step 1", move |_cx| async move {
                StepSuccess::new(()).into()
            })
            .register();

        engine
            .new_step("bar".to_owned(), 2, "Step 2", move |cx| async move {
                for _ in 0..20 {
                    cx.send_progress(StepProgress::with_current_and_total(
                        5,
                        20,
                        Default::default(),
                    ))
                    .await;

                    cx.send_progress(StepProgress::reset(
                        Default::default(),
                        "reset step 2",
                    ))
                    .await;

                    cx.send_progress(StepProgress::retry("retry step 2")).await;
                }
                StepSuccess::new(()).into()
            })
            .register();

        engine
            .new_step(
                "nested".to_owned(),
                3,
                "Step 3 (this is nested)",
                move |parent_cx| async move {
                    parent_cx
                        .with_nested_engine(|engine| {
                            define_nested_engine(&parent_cx, engine);
                            Ok(())
                        })
                        .await
                        .expect_err("this is expected to fail");

                    StepSuccess::new(()).into()
                },
            )
            .register();

        let log = logctx.log.clone();
        engine
            .new_step(
                "remote-nested".to_owned(),
                20,
                "Step 4 (remote nested)",
                move |cx| async move {
                    let (sender, mut receiver) = mpsc::channel(16);
                    let mut engine = UpdateEngine::new(&log, sender);
                    define_remote_nested_engine(&mut engine, 20);

                    let mut buffer = EventBuffer::default();

                    let mut execute_fut = std::pin::pin!(engine.execute());
                    let mut execute_done = false;
                    loop {
                        tokio::select! {
                            res = &mut execute_fut, if !execute_done => {
                                res.expect("remote nested engine completed successfully");
                                execute_done = true;
                            }
                            Some(event) = receiver.recv() => {
                                // Generate complete reports to ensure deduping
                                // happens within StepContexts.
                                buffer.add_event(event);
                                cx.send_nested_report(buffer.generate_report()).await?;
                            }
                            else => {
                                break;
                            }
                        }
                    }

                    StepSuccess::new(()).into()
                },
            )
            .register();

        // The step index here (100) is large enough to be higher than all nested
        // steps.
        engine
            .new_step("baz".to_owned(), 100, "Step 5", move |_cx| async move {
                StepSuccess::new(()).into()
            })
            .register();

        engine.execute().await.expect("execution successful");
        let generated_events: Vec<_> =
            ReceiverStream::new(receiver).collect().await;

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
                |buffer, event| buffer.add_event(event.clone()),
                WithDeltas::No,
            )
            .unwrap();

        test_cx
            .run_filtered_test(
                "progress events skipped",
                |buffer, event| {
                    if let Event::Step(event) = event {
                        buffer.add_step_event(event.clone());
                    }
                },
                WithDeltas::Both,
            )
            .unwrap();

        test_cx
            .run_filtered_test(
                "low-priority events skipped",
                |buffer, event| match event {
                    Event::Step(event) => {
                        if event.kind.priority() == StepEventPriority::Low {
                            buffer.add_step_event(event.clone());
                        }
                    }
                    Event::Progress(event) => {
                        buffer.add_progress_event(event.clone());
                    }
                },
                WithDeltas::Both,
            )
            .unwrap();

        test_cx
            .run_filtered_test(
                "low-priority and progress events skipped",
                |buffer, event| match event {
                    Event::Step(event) => {
                        if event.kind.priority() == StepEventPriority::Low {
                            buffer.add_step_event(event.clone());
                        }
                    }
                    Event::Progress(_) => {
                        // Don't add progress events either.
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
            Self { root_execution_id, generated_events, generated_step_events }
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
                    let report = buffer.generate_report_since(last_seen);
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
                    last_seen = report.last_seen;

                    // Call last_seen without feeding a new event in to ensure that
                    // a report with no step events is produced.
                    let report = buffer.generate_report_since(last_seen);
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
            mut event_fn: impl FnMut(&mut EventBuffer<TestSpec>, &Event<TestSpec>),
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
            mut event_fn: impl FnMut(&mut EventBuffer<TestSpec>, &Event<TestSpec>),
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
                (event_fn)(&mut buffer, event);
                buffer.add_event(event.clone());
                let report = match last_seen_opt {
                    Some(last_seen) => buffer.generate_report_since(last_seen),
                    None => buffer.generate_report(),
                };

                let is_last_event = i == self.generated_events.len() - 1;
                if let Some(last_seen) = &mut last_seen_opt {
                    *last_seen = report.last_seen;
                }

                self.assert_general_properties(&buffer, &report, is_last_event)
                    .with_context(|| {
                        format!(
                            "{description}, at index {i}, properties not met"
                        )
                    })
                    .unwrap();

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
                        summary[&root_execution_id].execution_status,
                        ExecutionStatus::Completed { .. },
                    ),
                    "this is the last event so ExecutionStatus must be completed"
                );
                // There are two nested engines.
                ensure!(
                    summary.len() == 3,
                    "two nested engines must be defined"
                );

                let (_, nested_summary) = summary
                    .get_index(1)
                    .expect("this is the first nested engine");
                ensure!(
                    matches!(
                        nested_summary.execution_status,
                        ExecutionStatus::Failed { .. },
                    ),
                    "for this engine, the ExecutionStatus must be failed"
                );

                let (_, nested_summary) = summary
                    .get_index(2)
                    .expect("this is the second nested engine");
                ensure!(
                    matches!(
                        nested_summary.execution_status,
                        ExecutionStatus::Completed { .. },
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

    fn define_nested_engine<'a>(
        parent_cx: &'a StepContext<TestSpec>,
        engine: &mut UpdateEngine<'a, TestSpec>,
    ) {
        engine
            .new_step(
                "nested-foo".to_owned(),
                4,
                "Nested step 1",
                move |cx| async move {
                    parent_cx
                        .send_progress(StepProgress::with_current_and_total(
                            1,
                            3,
                            Default::default(),
                        ))
                        .await;
                    cx.send_progress(
                        StepProgress::progress(Default::default()),
                    )
                    .await;
                    StepSuccess::new(()).into()
                },
            )
            .register();

        engine
            .new_step::<_, _, ()>(
                "nested-bar".to_owned(),
                5,
                "Nested step 2 (fails)",
                move |cx| async move {
                    parent_cx
                        .send_progress(StepProgress::with_current_and_total(
                            2,
                            3,
                            Default::default(),
                        ))
                        .await;

                    cx.send_progress(StepProgress::with_current(
                        20,
                        Default::default(),
                    ))
                    .await;

                    bail!("failing step")
                },
            )
            .register();
    }

    fn define_remote_nested_engine(
        engine: &mut UpdateEngine<'_, TestSpec>,
        start_id: usize,
    ) {
        engine
            .new_step(
                "nested-foo".to_owned(),
                start_id + 1,
                "Nested step 1",
                move |cx| async move {
                    cx.send_progress(
                        StepProgress::progress(Default::default()),
                    )
                    .await;
                    StepSuccess::new(()).into()
                },
            )
            .register();

        engine
            .new_step::<_, _, ()>(
                "nested-bar".to_owned(),
                start_id + 2,
                "Nested step 2",
                move |cx| async move {
                    cx.send_progress(StepProgress::with_current(
                        20,
                        Default::default(),
                    ))
                    .await;

                    StepSuccess::new(()).into()
                },
            )
            .register();
    }
}
