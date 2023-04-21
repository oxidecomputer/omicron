// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use std::collections::{hash_map, HashMap, VecDeque};

use derive_where::derive_where;
use petgraph::prelude::*;

use crate::{
    events::{
        Event, EventReport, ProgressEvent, ProgressEventKind, StepEvent,
        StepEventIsTerminal, StepEventKind, StepEventPriority,
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
        // Gather step events across all keys.
        let mut step_events = self.event_store.high_priority.clone();
        let mut progress_events = Vec::new();
        for value in self.event_store.map.values() {
            step_events.extend(value.low_priority.iter().cloned());
            progress_events.push(value.progress_event.clone());
        }

        // Sort events by their total duration. We assume that total_elapsed is
        // monotonically increasing. We should come up with a better sort
        // algorithm in the future, based on the actual event kinds.
        step_events.sort_by(|a, b| a.total_elapsed.cmp(&b.total_elapsed));
        progress_events.sort_by(|a, b| a.total_elapsed.cmp(&b.total_elapsed));

        EventReport { step_events, progress_events }
    }

    /// Clears events returned by the last call to [`Self::generate_report`].
    ///
    /// Call this after successfully sending a report over the wire.
    pub fn on_report_processed(&mut self) {
        self.event_store.clear_step_events();
        // Do *not* clear progress events -- we always want to keep returning at
        // least one of them, and have them be cleared in the end once a step or
        // execution is complete.
    }

    pub fn add_progress_event(&mut self, event: ProgressEvent<S>) {
        self.event_store.handle_progress_event(event);
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum CompletedIndex {
    LastSeen(usize),
    // Terminal is greater than any LastSeen.
    Terminal,
}

#[derive_where(Clone, Debug, Default)]
struct EventStore<S: StepSpec> {
    // A tree where edges are from parent event keys to child nested event keys.
    //
    // While petgraph seems like overkill at first, it results in really
    // straightforward algorithms below compared to alternatives like storing
    // trees using Box pointers.
    event_tree: DiGraphMap<EventKey, ()>,
    high_priority: Vec<StepEvent<S>>,
    map: HashMap<EventKey, EventMapValue<S>>,
    last_completed_index: HashMap<ExecutionId, CompletedIndex>,
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
                        // Handle duplicate events.
                        if self.high_priority.last() != Some(&event) {
                            self.high_priority.push(event);
                        }
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
                            entry.insert(EventMapValue::new(progress_event));
                            // Handle duplicate events.
                            if self.high_priority.last() != Some(&event) {
                                self.high_priority.push(event);
                            }
                        }
                        (StepEventPriority::High, None) => {
                            // Handle duplicate events.
                            if self.high_priority.last() != Some(&event) {
                                self.high_priority.push(event);
                            }
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
        parent_node: Option<EventKey>,
    ) -> Option<EventKey> {
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
                let key = EventKey {
                    execution_id: event.execution_id,
                    index: step.info.index,
                };
                self.last_completed_index.insert(
                    event.execution_id,
                    CompletedIndex::LastSeen(step.info.index),
                );
                // Clear all events for this ID and any child events.
                self.clear_event_key(key);

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
                let key = EventKey {
                    execution_id: event.execution_id,
                    index: step.info.index,
                };
                Some(key)
            }
            StepEventKind::ExecutionCompleted { last_step: step, .. }
            | StepEventKind::ExecutionFailed { failed_step: step, .. } => {
                // This is a terminal event: clear all progress for this
                // execution ID and any nested events.
                self.last_completed_index
                    .insert(event.execution_id, CompletedIndex::Terminal);
                self.clear_execution_id(event.execution_id);
                let key = EventKey {
                    execution_id: event.execution_id,
                    index: step.info.index,
                };
                Some(key)
            }
            StepEventKind::Nested { step, event: nested_event, .. } => {
                // Recurse and find any nested events.
                let parent_node = EventKey {
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
        parent_node: Option<EventKey>,
    ) -> Option<EventKey> {
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
    ) -> Option<EventKey> {
        if Some(&CompletedIndex::LastSeen(index))
            <= self.last_completed_index.get(&execution_id)
        {
            // We've already seen this step be completed. Ignore this
            // progress event.
            return None;
        }
        Some(self.event_tree.add_node(EventKey { execution_id, index }))
    }

    fn clear_step_events(&mut self) {
        // Keep terminal events since it's nice to always return the fact that
        // an engine has completed.
        self.high_priority.retain(|event| {
            matches!(
                event.kind.is_terminal(),
                StepEventIsTerminal::Terminal { .. }
            )
        });
        // Also clear low-priority step events across all keys.
        for value in self.map.values_mut() {
            value.low_priority.clear();
        }
    }

    fn clear_event_key(&mut self, key: EventKey) {
        // Remove this node and anything reachable from it.
        let mut dfs = DfsPostOrder::new(&self.event_tree, key);
        while let Some(key) = dfs.next(&self.event_tree) {
            // This will remove the node as well as all the edges into it.
            self.event_tree.remove_node(key);
            self.map.remove(&key);
        }
    }

    fn clear_execution_id(&mut self, execution_id: ExecutionId) {
        let mut dfs = DfsPostOrder::empty(&self.event_tree);
        // Push all nodes that start with the execution ID into the stack.
        dfs.stack.extend(
            self.map.keys().filter(|k| k.execution_id == execution_id).copied(),
        );
        while let Some(key) = dfs.next(&self.event_tree) {
            // This will remove the node as well as all the edges into it.
            self.event_tree.remove_node(key);
            self.map.remove(&key);
        }
    }
}

/// The list of events for a particular key.
#[derive_where(Clone, Debug)]
struct EventMapValue<S: StepSpec> {
    low_priority: VecDeque<StepEvent<S>>,
    progress_event: ProgressEvent<S>,
}

impl<S: StepSpec> EventMapValue<S> {
    fn new(progress_event: ProgressEvent<S>) -> Self {
        Self { low_priority: VecDeque::new(), progress_event }
    }

    fn add_low_priority_step_event(
        &mut self,
        event: StepEvent<S>,
        max_low_priority: usize,
    ) {
        assert_eq!(
            event.kind.priority(),
            StepEventPriority::Low,
            "EventMapValue only handles low-priority step events"
        );
        // Limit the number of events to the maximum low priority, ejecting the
        // oldest event if necessary.
        if self.low_priority.back() != Some(&event) {
            self.low_priority.push_front(event);
            while self.low_priority.len() > max_low_priority {
                self.low_priority.pop_front();
            }
        }
    }

    fn set_progress(&mut self, progress_event: ProgressEvent<S>) {
        self.progress_event = progress_event;
    }
}

/// A unique identifier for a group of step or progress events.
#[derive(Copy, Clone, Debug, Hash, Eq, PartialEq, Ord, PartialOrd)]
struct EventKey {
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
        let generated_step_events: Vec<_> = generated_events
            .iter()
            .filter_map(|event| match event {
                Event::Step(event) => Some(event.clone()),
                Event::Progress(_) => None,
            })
            .collect();

        // Feed events in one by one, marking reports as processed after each
        // event.
        {
            let mut buffer: EventBuffer<TestSpec> =
                EventBuffer::new(MAX_LOW_PRIORITY);
            let mut reported_step_events = Vec::new();

            for (i, event) in generated_events.iter().enumerate() {
                buffer.add_event(event.clone());
                let report = buffer.generate_report();
                let is_last_event = i == generated_events.len() - 1;
                assert_general_properties(&buffer, &report, is_last_event)
                .with_context(|| {
                    format!(
                        "WITH reports processed, properties not met at index {i}"
                    )
                })
                .unwrap();
                reported_step_events.extend(report.step_events);
                buffer.on_report_processed();
            }

            assert_eq!(
                generated_step_events, reported_step_events,
                "all generated step events were reported"
            );
        }

        let test_cx = BufferTestContext::new(generated_events);

        test_cx
            .run_all_elements_test(
                "all events passed in one-by-one",
                |buffer, event| buffer.add_event(event.clone()),
            )
            .unwrap();

        test_cx
            .run_all_elements_test("all events duplicated", |buffer, event| {
                buffer.add_event(event.clone());
                buffer.add_event(event.clone());
            })
            .unwrap();

        test_cx
            .run_filtered_test(
                "all events passed in",
                |buffer, event| buffer.add_event(event.clone()),
                MarkReportProcessed::No,
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
                MarkReportProcessed::Both,
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
                MarkReportProcessed::Both,
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
                MarkReportProcessed::Both,
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
        ) -> anyhow::Result<()> {
            let mut buffer: EventBuffer<TestSpec> =
                EventBuffer::new(MAX_LOW_PRIORITY);
            let mut reported_step_events = Vec::new();

            for (i, event) in self.generated_events.iter().enumerate() {
                (event_fn)(&mut buffer, event);
                let report = buffer.generate_report();
                let is_last_event = i == self.generated_events.len() - 1;
                assert_general_properties(&buffer, &report, is_last_event)
                    .with_context(|| {
                        format!(
                            "with {description}, for index {i}, \
                             properties not met"
                        )
                    })
                    .unwrap();
                reported_step_events.extend(report.step_events);
                buffer.on_report_processed();
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
            mark_report_processed: MarkReportProcessed,
        ) -> anyhow::Result<()> {
            match mark_report_processed {
                MarkReportProcessed::Yes => {
                    self.run_filtered_test_inner(&mut event_fn, true)
                        .context(event_fn_description.to_owned())?;
                }
                MarkReportProcessed::No => {
                    self.run_filtered_test_inner(&mut event_fn, false)
                        .context(event_fn_description.to_owned())?;
                }
                MarkReportProcessed::Both => {
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
            mark_report_processed: bool,
        ) -> anyhow::Result<()> {
            let description =
                format!("with mark_report_processed = {mark_report_processed}");
            let mut buffer = EventBuffer::new(MAX_LOW_PRIORITY);
            let mut last_high_priority = Vec::new();

            // If we're marking reports as processed, create a buffer that
            // doesn't drop events.
            let mut receive_buffer =
                mark_report_processed.then(|| EventBuffer::new(1024));

            for (i, event) in self.generated_events.iter().enumerate() {
                (event_fn)(&mut buffer, event);
                buffer.add_event(event.clone());
                let report = buffer.generate_report();
                let is_last_event = i == self.generated_events.len() - 1;
                if mark_report_processed {
                    buffer.on_report_processed();
                }

                assert_general_properties(&buffer, &report, is_last_event)
                    .with_context(|| {
                        format!(
                            "{description}, at index {i}, properties not met"
                        )
                    })
                    .unwrap();

                let this_step_events =
                    if let Some(receive_buffer) = receive_buffer.as_mut() {
                        receive_buffer.add_event_report(report);
                        receive_buffer.generate_report().step_events
                    } else {
                        report.step_events
                    };
                // Ensure that all high-priority events reported so far are seen.
                let this_high_priority: Vec<_> = this_step_events
                    .iter()
                    .filter(|event| {
                        event.kind.priority() == StepEventPriority::High
                    })
                    .cloned()
                    .collect();

                if this_high_priority.len() == last_high_priority.len() {
                    // event is not a high-priority event. All old high-priority
                    // events must be reported as well.
                    ensure!(
                        this_high_priority == last_high_priority,
                        "{description}, at index {i}, all high-priority events reported"
                    );
                } else if this_high_priority.len()
                    == last_high_priority.len() + 1
                {
                    // The first N events must match.
                    ensure!(
                        &this_high_priority[0..last_high_priority.len()]
                            == &last_high_priority,
                        "{description}, at index {i}, all old high-priority events reported"
                    );
                }

                last_high_priority = this_high_priority;
            }

            Ok(())
        }
    }

    #[derive(Copy, Clone, Debug)]
    #[allow(unused)]
    enum MarkReportProcessed {
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

        // Ensure that the internal event tree has one root.
        let root_keys: Vec<_> = buffer
            .event_store
            .event_tree
            .nodes()
            .filter(|node| {
                buffer
                    .event_store
                    .event_tree
                    .neighbors_directed(*node, Direction::Incoming)
                    .count()
                    == 0
            })
            .collect();
        if !is_last_event {
            if root_keys.len() != 1 {
                bail!("expected 1 root key, found {root_keys:?}");
            }
        } else {
            if !root_keys.is_empty() {
                bail!("expected no root keys since this is the last event, found {root_keys:?}");
            }
        }

        Ok(())
    }

    fn progress_event_key<S: StepSpec>(event: &ProgressEvent<S>) -> EventKey {
        match &event.kind {
            ProgressEventKind::WaitingForProgress { step, .. }
            | ProgressEventKind::Progress { step, .. } => EventKey {
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
