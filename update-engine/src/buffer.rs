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

    /// Returns information about each step, as currently tracked by the buffer,
    /// in order of when the events were first defined.
    pub fn steps<'a>(&'a self) -> Vec<(StepKey, &'a EventMapValue<S>)> {
        let mut items: Vec<_> =
            self.event_store.event_map_value_dfs().collect();
        items.sort_unstable_by_key(|(_, value)| value.sort_key());
        items
    }

    /// Returns information about the given step, as currently tracked by the
    /// buffer.
    pub fn get(&self, step_key: &StepKey) -> Option<&EventMapValue<S>> {
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
        for value in self.event_store.map.values() {
            step_events.extend(value.step_events_since(last_seen).cloned());
            progress_events.extend(value.step_status.progress_event().cloned());
        }

        // Sort events.
        step_events.sort_unstable_by_key(|event| event.event_index);
        progress_events.sort_unstable_by_key(|event| event.total_elapsed);
        if let Some(last) = step_events.last() {
            // Only update last_seen if there are new step events (otherwise it
            // stays the same).
            last_seen = Some(last.event_index);
        }

        EventReport { step_events, progress_events, last_seen }
    }

    /// Returns true if any further step events are pending since `last_seen`.
    ///
    /// This does not currently care about pending progress events, just pending
    /// step events. A typical use for this is to check that all step events
    /// have been reported before a sender shuts down.
    pub fn has_pending_events_since(&self, last_seen: Option<usize>) -> bool {
        for value in self.event_store.map.values() {
            if value.step_events_since(last_seen).next().is_some() {
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
    // A tree where edges are from parent event keys to child nested event keys.
    //
    // While petgraph seems like overkill at first, it results in really
    // straightforward algorithms below compared to alternatives like storing
    // trees using Box pointers.
    event_tree: DiGraphMap<EventTreeNode, ()>,
    root_execution_id: Option<ExecutionId>,
    map: HashMap<StepKey, EventMapValue<S>>,
}

impl<S: StepSpec> EventStore<S> {
    /// Returns a DFS of event map values.
    fn event_map_value_dfs(
        &self,
    ) -> impl Iterator<Item = (StepKey, &EventMapValue<S>)> + '_ {
        self.root_execution_id.into_iter().flat_map(|execution_id| {
            // Construct a DFS.
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

        let actions = self.recurse_for_step_event(&event);
        if let Some(new_execution) = actions.new_execution {
            self.root_execution_id = Some(new_execution.execution_id);
            for (new_step_key, new_step, nest_level) in
                new_execution.steps_to_add
            {
                // These are brand new steps so their keys shouldn't exist in the
                // map. But if they do, don't overwrite them.
                self.map.entry(new_step_key).or_insert_with(|| {
                    EventMapValue::new(new_step, event.event_index, nest_level)
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
                if event.kind.priority() == StepEventPriority::High {
                    value.add_high_priority_step_event(event);
                } else {
                    value.add_low_priority_step_event(event, max_low_priority);
                }
            }
        }
    }

    fn handle_progress_event(&mut self, event: ProgressEvent<S>) {
        if matches!(event.kind, ProgressEventKind::Unknown) {
            // Ignore unknown events.
            return;
        }

        if let Some(key) = self.recurse_for_progress_event(&event) {
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
                    let step_node = self.add_step_node(step_key);
                    self.event_tree.add_edge(root_node, step_node, ());
                    let step_info = step.clone().into_generic();
                    steps_to_add.push((step_key, step_info, 0));
                }
                new_execution = Some(NewExecutionAction {
                    execution_id: event.execution_id,
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
            StepEventKind::Nested { step, event: nested_event, .. } => {
                // Recurse and find any nested events.
                let parent_key = StepKey {
                    execution_id: event.execution_id,
                    index: step.info.index,
                };
                let actions = self.recurse_for_step_event(nested_event);
                if let Some(nested_new_execution) = actions.new_execution {
                    // Add an edge from the parent node to the new execution's root node.
                    self.event_tree.add_edge(
                        EventTreeNode::Step(parent_key),
                        EventTreeNode::Root(nested_new_execution.execution_id),
                        (),
                    );
                    let mut steps_to_add = Vec::new();
                    for (new_step_key, new_step, nest_level) in
                        nested_new_execution.steps_to_add
                    {
                        steps_to_add.push((
                            new_step_key,
                            new_step,
                            nest_level + 1,
                        ));
                    }

                    new_execution = Some(NewExecutionAction {
                        execution_id: nested_new_execution.execution_id,
                        steps_to_add,
                    });
                }
                (actions.step_key, actions.progress_key)
            }
            StepEventKind::NoStepsDefined | StepEventKind::Unknown => {
                (None, None)
            }
        };

        RecurseActions { new_execution, step_key, progress_key }
    }

    fn recurse_for_progress_event<S2: StepSpec>(
        &mut self,
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
                self.recurse_for_progress_event(nested_event)
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
            // Completion status only applies to the root key.
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
        if let Some(value) = self.map.get_mut(&root_key) {
            // Failure status only applies to the root key.
            value.mark_failed(Some(info));
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
                    value.mark_failed(None);
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
                    value.mark_will_not_be_run(root_key);
                }
            }
        }
    }
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

    // New steps to add, generated by ExecutionStarted events.
    // The triple is:
    // * step key
    // * step info
    // * nest level
    steps_to_add: Vec<(StepKey, StepInfo<NestedSpec>, usize)>,
}

/// The list of events for a particular key.
#[derive_where(Clone, Debug)]
pub struct EventMapValue<S: StepSpec> {
    step_info: StepInfo<NestedSpec>,
    nest_level: usize,
    defined_at_event: usize,
    // Invariant: stored in order sorted by event_index.
    high_priority: Vec<StepEvent<S>>,
    step_status: StepStatus<S>,
}

impl<S: StepSpec> EventMapValue<S> {
    fn new(
        step_info: StepInfo<NestedSpec>,
        defined_at_event: usize,
        nest_level: usize,
    ) -> Self {
        Self {
            step_info,
            defined_at_event,
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

    fn sort_key(&self) -> impl Ord {
        (self.defined_at_event, self.step_info.index)
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
        match self
            .high_priority
            .binary_search_by(|probe| probe.event_index.cmp(&event.event_index))
        {
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
                match low_priority.binary_search_by(|probe| {
                    probe.event_index.cmp(&event.event_index)
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
            | StepStatus::WillNotBeRun { .. } => {
                // Ignore the status if the step has already been marked
                // terminated.
            }
        }
    }

    fn mark_failed(&mut self, status: Option<FailureInfo>) {
        match self.step_status {
            StepStatus::NotStarted | StepStatus::Running { .. } => {
                self.step_status = StepStatus::Failed { info: status };
            }
            StepStatus::Completed { .. }
            | StepStatus::Failed { .. }
            | StepStatus::WillNotBeRun { .. } => {
                // Ignore the status if the step has already been marked
                // terminated.
            }
        }
    }

    fn mark_will_not_be_run(&mut self, step_that_failed: StepKey) {
        match self.step_status {
            StepStatus::NotStarted => {
                self.step_status =
                    StepStatus::WillNotBeRun { step_that_failed };
            }
            StepStatus::Running { .. } => {
                // This is a weird situation. We should never encounter it in
                // normal use -- if we do encounter it, just ignore it.
            }
            StepStatus::Completed { .. }
            | StepStatus::Failed { .. }
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

    /// The step will not be executed because a prior step failed.
    WillNotBeRun {
        /// The step that failed and caused this step to not be run.
        step_that_failed: StepKey,
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
            | Self::WillNotBeRun { .. } => Either::Right(std::iter::empty()),
        }
    }

    /// Returns the associated progress event for this step, if any.
    pub fn progress_event(&self) -> Option<&ProgressEvent<S>> {
        match self {
            Self::Running { progress_event, .. } => Some(progress_event),
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
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;

    use crate::{
        events::StepProgress, test_utils::TestSpec, StepContext, StepResult,
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
                StepResult::success((), Default::default())
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
                StepResult::success((), Default::default())
            })
            .register();

        engine
            .new_step(
                "nested".to_owned(),
                3,
                "Nested step",
                move |parent_cx| async move {
                    parent_cx
                        .with_nested_engine(|engine| {
                            define_nested_engine(&parent_cx, engine);
                            Ok(())
                        })
                        .await
                        .expect_err("this is expected to fail");

                    StepResult::success((), Default::default())
                },
            )
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
        generated_events: Vec<Event<TestSpec>>,
        // Data derived from generated_events.
        generated_step_events: Vec<StepEvent<TestSpec>>,
    }

    impl BufferTestContext {
        fn new(generated_events: Vec<Event<TestSpec>>) -> Self {
            let generated_step_events: Vec<_> = generated_events
                .iter()
                .filter_map(|event| match event {
                    Event::Step(event) => Some(event.clone()),
                    Event::Progress(_) => None,
                })
                .collect();
            Self { generated_events, generated_step_events }
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
                    assert_general_properties(&buffer, &report, is_last_event)
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

                assert_general_properties(&buffer, &report, is_last_event)
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
    }

    #[derive(Copy, Clone, Debug)]
    #[allow(unused)]
    enum WithDeltas {
        Yes,
        No,
        Both,
    }

    fn assert_general_properties<S: StepSpec>(
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
            if !buffer.event_store.event_tree.contains_node(key) {
                bail!("progress event key {key:?} not found in event tree");
            }
            if !buffer.event_store.map.contains_key(&key) {
                bail!("progress event key {key:?} not found in event map");
            }
        }

        // Ensure that the internal event tree has one root that's currently
        // running.
        let running_root_keys: Vec<_> = buffer
            .event_store
            .event_tree
            .nodes()
            .filter(|node| {
                let is_root = buffer
                    .event_store
                    .event_tree
                    .neighbors_directed(*node, Direction::Incoming)
                    .count()
                    == 0;
                is_root
                    && buffer
                        .event_store
                        .map
                        .get(&node)
                        .expect("event key must exist")
                        .step_status
                        .is_running()
            })
            .collect();
        if !is_last_event {
            if running_root_keys.len() != 1 {
                bail!(
                    "expected 1 running root key, found {running_root_keys:?}"
                );
            }
        } else {
            if !running_root_keys.is_empty() {
                bail!(
                    "expected no root keys since this is \
                     the last event, found {running_root_keys:?}"
                );
            }
        }

        Ok(())
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
                1,
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
                    StepResult::success((), Default::default())
                },
            )
            .register();

        engine
            .new_step::<_, _, ()>(
                "nested-bar".to_owned(),
                2,
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
}
