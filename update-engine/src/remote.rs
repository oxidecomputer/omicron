// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use std::{collections::HashMap, time::Duration};

use derive_where::derive_where;
use petgraph::prelude::*;

use crate::{
    events::{
        Event, ProgressEvent, ProgressEventKind, RemoteReport, StepEvent,
        StepEventIsTerminal, StepEventKind, StepEventPriority,
    },
    ExecutionId, StepSpec,
};

/// A receiver for events that will be sent out to a remote process.
///
/// This coalesces and drops irrelevant events as new ones come in.
#[derive_where(Clone, Debug)]
pub struct EventCoalescer<S: StepSpec> {
    execution_id: ExecutionId,
    step_events: Vec<StepEvent<S>>,
    // This is a hierarchical map, where each value contains a map of nested
    // keys. Clearing out a progress event also clears out all the events underneath it.
    progress_events: ProgressEventMap<S>,
    low_priority_limit: usize,
    current_low_priority: usize,
}

impl<S: StepSpec> EventCoalescer<S> {
    /// Creates a new event coalescer.
    ///
    /// `low_priority_limit` determines how many low-priority events are
    /// retained.
    pub fn new(execution_id: ExecutionId, low_priority_limit: usize) -> Self {
        Self {
            execution_id,
            step_events: Vec::new(),
            progress_events: ProgressEventMap::new(),
            low_priority_limit,
            current_low_priority: 0,
        }
    }

    /// Adds an [`Event`] to the coalescer.
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

    /// Adds a [`StepEvent`] to the coalescer.
    ///
    /// This might cause older low-priority events to fall off the list.
    pub fn add_step_event(&mut self, event: StepEvent<S>) {
        self.progress_events.handle_step_event(&event);
        // If this is a low-priority event, eject the oldest low-priority event.
        // (This can probably use a better algorithm in the future.)
        if event.kind.priority() == StepEventPriority::Low
            && self.current_low_priority >= self.low_priority_limit
        {
            let mut ejected = false;
            self.step_events.retain(|event| {
                if ejected {
                    return true;
                }
                if event.kind.priority() == StepEventPriority::Low {
                    ejected = true;
                    return false;
                }
                true
            });
        }
        self.step_events.push(event);
    }

    /// Generates a [`RemoteReport`] for this coalescer.
    ///
    /// This report can be serialized and sent over the wire.
    pub fn generate_report(&self, total_elapsed: Duration) -> RemoteReport<S> {
        let step_events = self.step_events.clone();
        // Sort progress event values by their total duration.
        let mut progress_events: Vec<_> =
            self.progress_events.map.values().cloned().collect();
        progress_events.sort_by(|a, b| a.total_elapsed.cmp(&b.total_elapsed));

        RemoteReport {
            execution_id: self.execution_id,
            total_elapsed,
            step_events,
            progress_events,
        }
    }

    /// Clears step events from the coalescer.
    ///
    /// Call this after successfully sending a report over the wire.
    pub fn clear_step_events(&mut self) {
        // Keep terminal events since they tend to be of most interest.
        self.step_events.retain(|event| {
            matches!(
                event.kind.is_terminal(),
                StepEventIsTerminal::Terminal { .. }
            )
        });
        // Do *not* clear progress events -- we always want to keep returning at
        // least one of them, and have them be cleared in the end once a step or
        // execution is complete.

        // A terminal event is always high priority, so we can reset
        // current_low_priority to 0.
        self.current_low_priority = 0;
    }

    pub fn add_progress_event(&mut self, event: ProgressEvent<S>) {
        self.progress_events.handle_progress_event(event);
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum CompletedIndex {
    LastSeen(usize),
    // Terminal is greater than any LastSeen.
    Terminal,
}

#[derive_where(Clone, Debug, Default)]
struct ProgressEventMap<S: StepSpec> {
    // A tree where edges are from parent events to child events.
    event_tree: DiGraphMap<ProgressEventKey, ()>,
    map: HashMap<ProgressEventKey, ProgressEvent<S>>,
    last_completed_index: HashMap<ExecutionId, CompletedIndex>,
}

impl<S: StepSpec> ProgressEventMap<S> {
    fn new() -> Self {
        Self::default()
    }

    /// Handles a step event.
    fn handle_step_event(&mut self, event: &StepEvent<S>) {
        if let Some((node, progress_event)) =
            self.recurse_for_step_event(event, None)
        {
            self.map.insert(node, progress_event);
        }
    }

    fn handle_progress_event(&mut self, event: ProgressEvent<S>) {
        if let Some(node) = self.recurse_for_progress_event(&event, None) {
            self.map.insert(node, event);
        }
    }

    /// This method recurses down the structure of an event, resetting all
    /// progress.
    ///
    /// If a progress event needs to be added to self.map, this returns the key
    /// and value.
    fn recurse_for_step_event<S2: StepSpec>(
        &mut self,
        event: &StepEvent<S2>,
        parent_node: Option<ProgressEventKey>,
    ) -> Option<(ProgressEventKey, ProgressEvent<S2>)> {
        match &event.kind {
            StepEventKind::ExecutionStarted { first_step, .. } => {
                // Register the start of progress here.
                let key =
                    self.add_node(event.execution_id, first_step.info.index)?;
                if let Some(parent_node) = parent_node {
                    self.event_tree.add_edge(parent_node, key, ());
                }
                let progress_event = ProgressEvent {
                    execution_id: event.execution_id,
                    total_elapsed: event.total_elapsed,
                    kind: ProgressEventKind::Waiting {
                        step: first_step.clone(),
                        attempt: 1,
                        step_elapsed: Duration::ZERO,
                        attempt_elapsed: Duration::ZERO,
                    },
                };
                Some((key, progress_event))
            }
            StepEventKind::StepCompleted { step, next_step, .. } => {
                let key = ProgressEventKey {
                    execution_id: event.execution_id,
                    index: step.info.index,
                };
                self.last_completed_index.insert(
                    event.execution_id,
                    CompletedIndex::LastSeen(step.info.index),
                );
                // Clear all events for this ID and any child events.
                self.remove(key);

                // Register the next step in the progress map.
                let next_key =
                    self.add_node(event.execution_id, next_step.info.index)?;
                if let Some(parent_node) = parent_node {
                    self.event_tree.add_edge(parent_node, next_key, ());
                }
                let progress_event = ProgressEvent {
                    execution_id: event.execution_id,
                    total_elapsed: event.total_elapsed,
                    kind: ProgressEventKind::Waiting {
                        step: next_step.clone(),
                        attempt: 1,
                        step_elapsed: Duration::ZERO,
                        attempt_elapsed: Duration::ZERO,
                    },
                };
                Some((next_key, progress_event))
            }
            StepEventKind::ProgressReset {
                step,
                attempt,
                step_elapsed,
                attempt_elapsed,
                ..
            } => {
                // Reset progress for the step in the progress map.
                let key = ProgressEventKey {
                    execution_id: event.execution_id,
                    index: step.info.index,
                };
                let progress_event = ProgressEvent {
                    execution_id: event.execution_id,
                    total_elapsed: event.total_elapsed,
                    kind: ProgressEventKind::Waiting {
                        step: step.clone(),
                        attempt: *attempt,
                        step_elapsed: *step_elapsed,
                        attempt_elapsed: *attempt_elapsed,
                    },
                };
                Some((key, progress_event))
            }
            StepEventKind::AttemptRetry {
                step,
                next_attempt,
                step_elapsed,
                attempt_elapsed,
                ..
            } => {
                // Reset progress for the step in the progress map.
                let key = ProgressEventKey {
                    execution_id: event.execution_id,
                    index: step.info.index,
                };
                let progress_event = ProgressEvent {
                    execution_id: event.execution_id,
                    total_elapsed: event.total_elapsed,
                    kind: ProgressEventKind::Waiting {
                        step: step.clone(),
                        attempt: *next_attempt,
                        step_elapsed: *step_elapsed,
                        attempt_elapsed: *attempt_elapsed,
                    },
                };
                Some((key, progress_event))
            }
            StepEventKind::NoStepsDefined
            | StepEventKind::ExecutionCompleted { .. }
            | StepEventKind::ExecutionFailed { .. } => {
                // This is a terminal event: clear all progress for this
                // execution ID and any nested events.
                self.last_completed_index
                    .insert(event.execution_id, CompletedIndex::Terminal);
                self.clear_execution_id(event.execution_id);
                None
            }
            StepEventKind::Nested {
                step,
                attempt,
                event: nested_event,
                step_elapsed,
                attempt_elapsed,
            } => {
                // Recurse and find any nested events.
                let parent_node = ProgressEventKey {
                    execution_id: event.execution_id,
                    index: step.info.index,
                };

                let (key, returned_event) = self
                    .recurse_for_step_event(nested_event, Some(parent_node))?;
                let progress_event = ProgressEvent {
                    execution_id: event.execution_id,
                    total_elapsed: event.total_elapsed,
                    kind: ProgressEventKind::Nested {
                        step: step.clone(),
                        attempt: *attempt,
                        event: Box::new(returned_event),
                        step_elapsed: *step_elapsed,
                        attempt_elapsed: *attempt_elapsed,
                    },
                };
                Some((key, progress_event))
            }
            StepEventKind::Unknown => None,
        }
    }

    fn recurse_for_progress_event<S2: StepSpec>(
        &mut self,
        event: &ProgressEvent<S2>,
        parent_node: Option<ProgressEventKey>,
    ) -> Option<ProgressEventKey> {
        match &event.kind {
            ProgressEventKind::Waiting { step, .. }
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
    ) -> Option<ProgressEventKey> {
        if Some(&CompletedIndex::LastSeen(index))
            <= self.last_completed_index.get(&execution_id)
        {
            // We've already seen this step be completed. Ignore this
            // progress event.
            return None;
        }
        Some(self.event_tree.add_node(ProgressEventKey { execution_id, index }))
    }

    fn remove(&mut self, key: ProgressEventKey) {
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

/// A unique identifier to key progress events against.
#[derive(Copy, Clone, Debug, Hash, Eq, PartialEq, Ord, PartialOrd)]
struct ProgressEventKey {
    execution_id: ExecutionId,
    index: usize,
}

#[cfg(test)]
mod tests {
    use anyhow::bail;
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
    async fn test_coalesce_valid_events() {
        let logctx = test_setup_log("test_coalesce_valid_events");
        // The channel is big enough to contain all possible events.
        let (sender, receiver) = mpsc::channel(512);
        let engine: UpdateEngine<TestSpec> =
            UpdateEngine::new(&logctx.log, sender);
        let execution_id = engine.execution_id();

        engine
            .new_step("foo".to_owned(), 1, "Step 1", move |_cx| async move {
                StepResult::success((), Default::default())
            })
            .register();

        engine
            .new_step("bar".to_owned(), 2, "Step 2", move |cx| async move {
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
        let events: Vec<_> = ReceiverStream::new(receiver).collect().await;
        let mut coalescer = EventCoalescer::new(execution_id, 20);
        for event in events {
            coalescer.add_event(event);
            let report = coalescer.generate_report(Duration::ZERO);
            coalescer.clear_step_events();
            println!("{report:#?}");
        }
    }

    fn assert_report_structure<S: StepSpec>(report: RemoteReport<S>) {
        // Ensure that there's just one progress event per report.
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
