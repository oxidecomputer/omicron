// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use std::collections::{hash_map, HashMap, VecDeque};

use derive_where::derive_where;
use either::Either;
use petgraph::prelude::*;

use crate::{
    events::{
        Event, EventReport, ProgressEvent, ProgressEventKind, StepEvent,
        StepEventKind, StepEventPriority,
    },
    ExecutionId, StepSpec,
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
        step_events.sort_by(|a, b| a.event_index.cmp(&b.event_index));
        progress_events.sort_by(|a, b| a.total_elapsed.cmp(&b.total_elapsed));
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

#[derive_where(Clone, Debug, Default)]
struct EventStore<S: StepSpec> {
    // A tree where edges are from parent event keys to child nested event keys.
    //
    // While petgraph seems like overkill at first, it results in really
    // straightforward algorithms below compared to alternatives like storing
    // trees using Box pointers.
    event_tree: DiGraphMap<StepKey, ()>,
    map: HashMap<StepKey, EventMapValue<S>>,
}

impl<S: StepSpec> EventStore<S> {
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

        if let Some(key) = self.recurse_for_step_event(&event, None) {
            match self.map.entry(key) {
                hash_map::Entry::Occupied(mut entry) => {
                    let value = entry.get_mut();
                    let progress_event = event.progress_event();
                    if event.kind.priority() == StepEventPriority::High {
                        value.add_high_priority_step_event(event);
                    } else {
                        value.add_low_priority_step_event(
                            event,
                            max_low_priority,
                        );
                    }
                    if let Some(progress) = progress_event {
                        value.set_progress(progress);
                    }
                }

                hash_map::Entry::Vacant(entry) => {
                    let priority = event.kind.priority();
                    let progress_event = event.progress_event();

                    match (priority, progress_event) {
                        (StepEventPriority::High, Some(progress_event)) => {
                            let value = entry
                                .insert(EventMapValue::new(progress_event));
                            value.add_high_priority_step_event(event);
                        }
                        (StepEventPriority::High, None) => {
                            // The only way to reach this branch is with a
                            // NoStepsDefined event.
                            entry
                                .insert(EventMapValue::no_steps_defined(event));
                        }
                        (StepEventPriority::Low, Some(progress_event)) => {
                            let value = entry
                                .insert(EventMapValue::new(progress_event));
                            value.add_low_priority_step_event(
                                event,
                                max_low_priority,
                            );
                        }
                        (StepEventPriority::Low, None) => {
                            // This branch is reached if:
                            //
                            // * This is a vacant entry, AND
                            // * this is a low-priority event, AND
                            // * the event doesn't have any progress associated
                            //   with it
                            //
                            // The only possibility is that this is an unknown
                            // event, which we already filtered out at the top
                            // of this function.
                            debug_assert!(
                                false,
                                "This branch cannot be reached"
                            );
                        }
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

        if let Some(key) = self.recurse_for_progress_event(&event, None) {
            match self.map.entry(key) {
                hash_map::Entry::Occupied(mut entry) => {
                    entry.get_mut().set_progress(event);
                }
                hash_map::Entry::Vacant(entry) => {
                    entry.insert(EventMapValue::new(event));
                }
            }
        }
    }

    /// Recurses down the structure of a step event, adding nodes to the event
    /// tree as required. Returns the event key for the next event, if one is
    /// available.
    fn recurse_for_step_event<S2: StepSpec>(
        &mut self,
        event: &StepEvent<S2>,
        parent_node: Option<StepKey>,
    ) -> Option<StepKey> {
        match &event.kind {
            StepEventKind::ExecutionStarted { first_step, .. } => {
                // Register the start of progress here.
                let key =
                    self.add_node(event.execution_id, first_step.info.index)?;
                if let Some(parent_node) = parent_node {
                    self.event_tree.add_edge(parent_node, key, ());
                }
                Some(key)
            }
            StepEventKind::StepCompleted { step, next_step, .. } => {
                let key = StepKey {
                    execution_id: event.execution_id,
                    index: step.info.index,
                };
                // Mark this key and all child keys completed.
                self.mark_event_key_completed(key);

                // Register the next step in the progress map.
                let next_key =
                    self.add_node(event.execution_id, next_step.info.index)?;
                if let Some(parent_node) = parent_node {
                    self.event_tree.add_edge(parent_node, next_key, ());
                }
                Some(next_key)
            }
            StepEventKind::ProgressReset { step, .. }
            | StepEventKind::AttemptRetry { step, .. } => {
                // Reset progress for the step in the progress map.
                let key = StepKey {
                    execution_id: event.execution_id,
                    index: step.info.index,
                };
                Some(key)
            }
            StepEventKind::ExecutionCompleted { last_step: step, .. }
            | StepEventKind::ExecutionFailed { failed_step: step, .. } => {
                // This is a terminal event: clear all progress for this
                // execution ID and any nested events.
                self.clear_execution_id(event.execution_id);
                let key = StepKey {
                    execution_id: event.execution_id,
                    index: step.info.index,
                };
                Some(key)
            }
            StepEventKind::Nested { step, event: nested_event, .. } => {
                // Recurse and find any nested events.
                let parent_node = StepKey {
                    execution_id: event.execution_id,
                    index: step.info.index,
                };
                self.recurse_for_step_event(nested_event, Some(parent_node))
            }
            StepEventKind::NoStepsDefined | StepEventKind::Unknown => None,
        }
    }

    fn recurse_for_progress_event<S2: StepSpec>(
        &mut self,
        event: &ProgressEvent<S2>,
        parent_node: Option<StepKey>,
    ) -> Option<StepKey> {
        match &event.kind {
            ProgressEventKind::WaitingForProgress { step, .. }
            | ProgressEventKind::Progress { step, .. } => {
                let key = self.add_node(event.execution_id, step.info.index)?;
                if let Some(parent_node) = parent_node {
                    self.event_tree.add_edge(parent_node, key, ());
                }
                Some(key)
            }
            ProgressEventKind::Nested { step, event: nested_event, .. } => {
                // Add a node for this event.
                let parent =
                    self.add_node(event.execution_id, step.info.index)?;

                // Add nodes for nested events.
                self.recurse_for_progress_event(nested_event, Some(parent))
            }
            ProgressEventKind::Unknown => None,
        }
    }

    fn add_node(
        &mut self,
        execution_id: ExecutionId,
        index: usize,
    ) -> Option<StepKey> {
        Some(self.event_tree.add_node(StepKey { execution_id, index }))
    }

    fn mark_event_key_completed(&mut self, key: StepKey) {
        // Remove this node and anything reachable from it.
        let mut dfs = DfsPostOrder::new(&self.event_tree, key);
        while let Some(key) = dfs.next(&self.event_tree) {
            if let Some(value) = self.map.get_mut(&key) {
                value.mark_completed();
            }
        }
    }

    fn clear_execution_id(&mut self, execution_id: ExecutionId) {
        let mut dfs = DfsPostOrder::empty(&self.event_tree);
        // Push all nodes that start with the execution ID into the stack.
        dfs.stack.extend(
            self.map.keys().filter(|k| k.execution_id == execution_id).copied(),
        );
        while let Some(key) = dfs.next(&self.event_tree) {
            if let Some(value) = self.map.get_mut(&key) {
                value.mark_completed();
            }
        }
    }
}

/// The list of events for a particular key.
#[derive_where(Clone, Debug)]
struct EventMapValue<S: StepSpec> {
    // Invariant: stored in order sorted by event_index.
    high_priority: Vec<StepEvent<S>>,
    step_status: StepStatus<S>,
}

impl<S: StepSpec> EventMapValue<S> {
    fn new(progress_event: ProgressEvent<S>) -> Self {
        Self {
            high_priority: Vec::new(),
            step_status: StepStatus::Running {
                low_priority: VecDeque::new(),
                progress_event,
            },
        }
    }

    fn no_steps_defined(step_event: StepEvent<S>) -> Self {
        assert_eq!(
            step_event.kind,
            StepEventKind::NoStepsDefined,
            "this constructor should only be called on NoStepsDefined"
        );
        Self {
            high_priority: vec![step_event],
            step_status: StepStatus::Completed,
        }
    }

    // Returns step events since the provided event index.
    //
    // Does not necessarily return results in sorted order.
    fn step_events_since(
        &self,
        last_seen: Option<usize>,
    ) -> impl Iterator<Item = &StepEvent<S>> {
        // partition_point is safe since pred is
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
        if let StepStatus::Running { low_priority, .. } = &mut self.step_status
        {
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
    }

    fn mark_completed(&mut self) {
        self.step_status = StepStatus::Completed;
    }

    fn set_progress(&mut self, current_progress: ProgressEvent<S>) {
        if let StepStatus::Running { progress_event, .. } =
            &mut self.step_status
        {
            *progress_event = current_progress;
        }
    }
}

/// The step status as last seen by events.
#[derive_where(Clone, Debug)]
enum StepStatus<S: StepSpec> {
    Running {
        // Invariant: stored in sorted order by index.
        low_priority: VecDeque<StepEvent<S>>,
        progress_event: ProgressEvent<S>,
    },
    Completed,
}

impl<S: StepSpec> StepStatus<S> {
    #[allow(unused)]
    fn is_running(&self) -> bool {
        matches!(self, Self::Running { .. })
    }

    fn low_priority(&self) -> impl Iterator<Item = &StepEvent<S>> {
        match self {
            Self::Running { low_priority, .. } => {
                Either::Left(low_priority.iter())
            }
            Self::Completed => Either::Right(std::iter::empty()),
        }
    }

    fn progress_event(&self) -> Option<&ProgressEvent<S>> {
        match self {
            Self::Running { progress_event, .. } => Some(progress_event),
            Self::Completed => None,
        }
    }
}

/// A unique identifier for a group of step or progress events.
#[derive(Copy, Clone, Debug, Hash, Eq, PartialEq, Ord, PartialOrd)]
struct StepKey {
    execution_id: ExecutionId,
    index: usize,
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
