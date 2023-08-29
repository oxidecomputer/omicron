// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use std::marker::PhantomData;
use std::sync::Mutex;
use std::{collections::HashMap, fmt};

use derive_where::derive_where;
use futures::FutureExt;
use tokio::sync::{mpsc, oneshot};

use crate::errors::NestedEngineError;
use crate::{
    errors::ExecutionError,
    events::{Event, EventReport, StepEventKind, StepProgress},
    NestedError, NestedSpec, StepSpec, UpdateEngine,
};
use crate::{EventBuffer, ExecutionId};

/// Context for a step's execution function.
///
/// This is passed into the function registered for a step, and can be used to
/// send progress updates as the function continues execution.
///
/// # Notes
///
/// `StepContext` deliberately does not implement `Clone`, to make it more
/// likely that it is dropped at the same time the future completes.
#[derive(Debug)]
pub struct StepContext<S: StepSpec> {
    log: slog::Logger,
    payload_sender: mpsc::Sender<StepContextPayload<S>>,
    token: StepHandleToken<S>,
    // This is keyed by root execution ID in case there are multiple nested
    // events taking place. Each `NestedEventBuffer` tracks one such execution
    // ID.
    nested_buffers: Mutex<HashMap<ExecutionId, NestedEventBuffer>>,
}

impl<S: StepSpec> StepContext<S> {
    pub(crate) fn new(
        log: &slog::Logger,
        payload_sender: mpsc::Sender<StepContextPayload<S>>,
    ) -> Self {
        Self {
            log: log.clone(),
            payload_sender,
            token: StepHandleToken::new(),
            nested_buffers: Default::default(),
        }
    }

    /// Sends a progress update to the update engine.
    #[inline]
    pub async fn send_progress(&self, progress: StepProgress<S>) {
        self.payload_sender
            .send(StepContextPayload::Progress(progress))
            .await
            .expect("our code always keeps the receiver open")
    }

    /// Sends a report from a nested engine, typically one running on a remote
    /// machine.
    ///
    /// Returns an error if a [`StepEventKind::ExecutionFailed`] event was seen.
    #[inline]
    pub async fn send_nested_report<S2: StepSpec>(
        &self,
        report: EventReport<S2>,
    ) -> Result<(), NestedEngineError<NestedSpec>> {
        let mut res = Ok(());
        let delta_report = if let Some(id) = report.root_execution_id {
            let mut nested_buffers = self.nested_buffers.lock().unwrap();
            Some(nested_buffers.entry(id).or_default().add_event_report(report))
        } else {
            // If there's no root execution ID set, report is expected to be
            // empty. However, report is untrusted data so we can't assert on
            // it. Instead, log this.
            if !report.step_events.is_empty() {
                slog::warn!(
                    self.log,
                    "received non-empty report with empty root execution ID";
                    "report" => ?report,
                );
            }
            None
        };

        if let Some(delta_report) = delta_report {
            for event in delta_report.step_events {
                match &event.kind {
                    StepEventKind::ExecutionFailed {
                        failed_step,
                        message,
                        causes,
                        ..
                    } => {
                        res = Err(NestedEngineError::StepFailed {
                            component: failed_step.info.component.clone(),
                            id: failed_step.info.id.clone(),
                            description: failed_step.info.description.clone(),
                            error: NestedError::new(
                                message.clone(),
                                causes.clone(),
                            ),
                        });
                    }
                    StepEventKind::ExecutionAborted {
                        aborted_step,
                        message,
                        ..
                    } => {
                        res = Err(NestedEngineError::Aborted {
                            component: aborted_step.info.component.clone(),
                            id: aborted_step.info.id.clone(),
                            description: aborted_step.info.description.clone(),
                            message: message.clone(),
                        });
                    }
                    StepEventKind::NoStepsDefined
                    | StepEventKind::ExecutionStarted { .. }
                    | StepEventKind::AttemptRetry { .. }
                    | StepEventKind::ProgressReset { .. }
                    | StepEventKind::StepCompleted { .. }
                    | StepEventKind::ExecutionCompleted { .. }
                    // Note: we do not care about nested failures or aborts.
                    // That's because the parent step might have restarted
                    // nested engines. Only top-level failures or aborts matter.
                    | StepEventKind::Nested { .. }
                    | StepEventKind::Unknown => {}
                }

                self.payload_sender
                    .send(StepContextPayload::Nested(Event::Step(event)))
                    .await
                    .expect("our code always keeps the receiver open");
            }

            for event in delta_report.progress_events {
                self.payload_sender
                    .send(StepContextPayload::Nested(Event::Progress(event)))
                    .await
                    .expect("our code always keeps the receiver open");
            }
        }

        res
    }

    /// Creates a nested execution engine.
    ///
    /// An individual step can generate other steps: these steps are treated as
    /// *nested*, and carry their own progress.
    pub async fn with_nested_engine<'a, 'this, F, S2>(
        &'this self,
        engine_fn: F,
    ) -> Result<CompletionContext<S2>, NestedEngineError<S2>>
    where
        'this: 'a,
        F: FnOnce(&mut UpdateEngine<'a, S2>) -> Result<(), S2::Error> + Send,
        S2: StepSpec + 'a,
    {
        let (sender, mut receiver) = mpsc::channel(128);
        let mut engine = UpdateEngine::new(&self.log, sender);
        // Create the engine's steps.
        (engine_fn)(&mut engine)
            .map_err(|error| NestedEngineError::Creation { error })?;

        // Now run the engine.
        let engine = engine.execute();
        tokio::pin!(engine);

        let mut result = None;
        let mut events_done = false;

        loop {
            tokio::select! {
                ret = &mut engine, if result.is_none() => {
                    match ret {
                        Ok(cx) => {
                            result = Some(Ok(cx));
                        }
                        Err(ExecutionError::EventSendError(_)) => {
                            unreachable!("we always keep the receiver open")
                        }
                        Err(ExecutionError::StepFailed { component, id, description, error }) => {
                            result = Some(Err(NestedEngineError::StepFailed { component, id, description, error }));
                        }
                        Err(ExecutionError::Aborted { component, id, description, message }) => {
                            result = Some(Err(NestedEngineError::Aborted { component, id, description, message }));
                        }
                    }
                }
                event = receiver.recv(), if !events_done => {
                    match event {
                        Some(event) => {
                            self.payload_sender.send(
                                StepContextPayload::Nested(event.into_generic())
                            )
                            .await
                            .expect("we always keep the receiver open");
                        }
                        None => {
                            events_done = true;
                        }
                    }
                }
                else => {
                    break;
                }
            }
        }

        result.expect("the loop only exits if result is set")
    }

    /// Retrieves a token used to fetch the value out of a [`StepHandle`].
    pub fn token(&self) -> &StepHandleToken<S> {
        &self.token
    }
}

/// Tracker for [`StepContext::send_nested_report`].
///
/// Nested event reports might contain events already seen in prior runs:
/// `NestedEventBuffer` deduplicates those events such that only deltas are sent
/// over the channel.
#[derive(Debug, Default)]
struct NestedEventBuffer {
    buffer: EventBuffer<NestedSpec>,
    last_seen: Option<usize>,
}

impl NestedEventBuffer {
    /// Adds an event report to the buffer, and generates a corresponding event
    /// report that can be used to send data upstream.
    fn add_event_report<S: StepSpec>(
        &mut self,
        report: EventReport<S>,
    ) -> EventReport<NestedSpec> {
        self.buffer.add_event_report(report.into_generic());
        let ret = self.buffer.generate_report_since(self.last_seen);
        self.last_seen = ret.last_seen;
        ret
    }
}

#[derive_where(Debug)]
pub(crate) enum StepContextPayload<S: StepSpec> {
    Progress(StepProgress<S>),
    Nested(Event<NestedSpec>),
}

/// Context for a step's metadata-generation function.
///
/// This is passed into the function registered to generate a step's metadata.
///
/// # Notes
///
/// `MetadataContext` deliberately does not implement `Clone`, to make it more
/// likely that it is dropped at the same time the future completes.
#[derive_where(Debug)]
pub struct MetadataContext<S: StepSpec> {
    token: StepHandleToken<S>,
}

impl<S: StepSpec> MetadataContext<S> {
    pub(crate) fn new() -> Self {
        Self { token: StepHandleToken::new() }
    }

    /// Retrieves a token used to fetch the value out of a [`StepHandle`].
    pub fn token(&self) -> &StepHandleToken<S> {
        &self.token
    }
}

/// Context returned by a successful
/// [`UpdateEngine::execute`](crate::UpdateEngine::execute).
///
/// This can be used to retrieve the value of a `StepHandle`. In the future, it
/// may also be extended to provide more information
#[derive_where(Debug)]
pub struct CompletionContext<S: StepSpec> {
    token: StepHandleToken<S>,
}

impl<S: StepSpec> CompletionContext<S> {
    pub(crate) fn new() -> Self {
        Self { token: StepHandleToken::new() }
    }

    /// Retrieves a token used to fetch the value out of a [`StepHandle`].
    pub fn token(&self) -> &StepHandleToken<S> {
        &self.token
    }
}

/// A token to retrieve the value within a [`StepHandle`].
///
/// For more information, see the documentation for [`StepHandle`].
#[derive_where(Debug)]
pub struct StepHandleToken<S>(PhantomData<S>);

impl<S> StepHandleToken<S> {
    fn new() -> Self {
        Self(PhantomData)
    }
}

/// A way to obtain a step's result.
///
/// This handle can be used to transfer data between steps.
///
/// `StepHandle`s must be awaited:
///
/// 1. Either within a future step,
/// 2. Or after the engine completes executing.
///
/// To ensure this, the only way to retrieve a `StepHandle` is with a
/// `StepHandleToken`, returned by:
///
/// * [`StepContext::token`]
/// * [`MetadataContext::token`]
/// * [`CompletionContext::token`]
///
/// It is important that `StepHandle`s never be awaited outside the context of a
/// step that comes afterwards -- doing so will cause an immediate deadlock.
///
/// Dropping a `StepHandle` has no effect on whether the step itself runs.
#[derive_where(Debug; T: fmt::Debug)]
pub struct StepHandle<T, S> {
    receiver: oneshot::Receiver<T>,
    _marker: PhantomData<S>,
}

impl<T, S> StepHandle<T, S> {
    /// Creates a `StepHandle` that immediately provides a value.
    ///
    /// The value is always available and can be used within any steps.
    pub fn ready(value: T) -> Self {
        let (sender, receiver) = oneshot::channel();
        // Can't use expect here because T doesn't implement Debug.
        if let Err(_) = sender.send(value) {
            unreachable!("we're holding the receiver open")
        }
        Self::new(receiver)
    }

    pub(crate) fn new(receiver: oneshot::Receiver<T>) -> Self {
        Self { receiver, _marker: PhantomData }
    }

    /// Resolves to the output from a step, `T`.
    pub async fn into_value(self, _token: &StepHandleToken<S>) -> T {
        self.receiver.await.expect("update-engine always sends a value")
    }

    /// Returns a shared handle.
    ///
    /// A `SharedStepHandle` can be used to retrieve the output of a future
    /// across multiple dependent steps.
    pub fn into_shared(self) -> SharedStepHandle<T, S>
    where
        T: Clone,
    {
        SharedStepHandle {
            receiver: self.receiver.shared(),
            _marker: self._marker,
        }
    }
}

/// A shared version of [`StepHandle`].
///
/// A `SharedStepHandle` is cloneable, and can be used to retrieve the output of
/// a step across multiple dependent steps.
#[derive_where(Debug; T: fmt::Debug)]
#[derive_where(Clone; T: Clone)]
pub struct SharedStepHandle<T, S> {
    receiver: futures::future::Shared<oneshot::Receiver<T>>,
    _marker: PhantomData<S>,
}

impl<T: Clone, S> SharedStepHandle<T, S> {
    /// Resolves to the output from a step, `T`.
    pub async fn into_value(self, _token: &StepHandleToken<S>) -> T {
        self.receiver.await.expect("update-engine always sends a value")
    }
}
