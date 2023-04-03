// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use std::{borrow::Cow, pin::Pin, sync::Mutex, task::Poll};

use debug_ignore::DebugIgnore;
use derive_where::derive_where;
use futures::{future::BoxFuture, prelude::*};
use linear_map::LinearMap;
use pin_project_lite::pin_project;
use tokio::{
    sync::{mpsc, oneshot},
    time::Instant,
};

use crate::{
    errors::ExecutionError,
    events::{
        Event, ProgressEvent, ProgressEventKind, StepComponentSummary,
        StepEvent, StepEventKind, StepInfo, StepInfoWithMetadata, StepOutcome,
        StepProgress,
    },
    AsError, StepSpec,
};

#[derive_where(Debug)]
pub struct UpdateEngine<'a, S: StepSpec> {
    // TODO: for now, this is a sequential series of steps. This can potentially
    // be a graph in the future.
    log: slog::Logger,
    // This is a mutex to allow borrows to steps to be held by both
    // ComponentRegistrar and NewStep at the same time. (This could also be a
    // `RefCell` if a `Send` bound isn't required.)
    //
    // There is an alternative way to do this that doesn't use a mutex but
    // involves no less than three lifetime parameters, which is excessive.
    steps: Mutex<Steps<'a, S>>,
}

impl<'a, S: StepSpec> UpdateEngine<'a, S> {
    /// Creates a new `UpdateEngine`.
    pub fn new(log: &slog::Logger) -> Self {
        Self {
            log: log.new(slog::o!("component" => "UpdateEngine")),
            steps: Default::default(),
        }
    }

    /// Adds a new step corresponding to the given component.
    pub fn new_step<F, Fut, T>(
        &self,
        component: S::Component,
        id: S::StepId,
        description: impl Into<Cow<'static, str>>,
        step_fn: F,
    ) -> NewStep<'_, 'a, S, T>
    where
        F: FnOnce(StepContext<S>) -> Fut + 'a,
        Fut: Future<Output = Result<StepResult<S, T>, S::Error>> + Send + 'a,
        T: Send + 'a,
    {
        self.for_component(component).new_step(id, description, step_fn)
    }

    /// Creates a [`ComponentRegistrar`] that contains the component for a given.
    pub fn for_component(
        &self,
        component: S::Component,
    ) -> ComponentRegistrar<'_, 'a, S> {
        ComponentRegistrar { steps: &self.steps, component }
    }

    /// Executes the list of steps. The sender is a list of steps.
    pub async fn execute(
        self,
        sender: mpsc::Sender<Event<S>>,
    ) -> Result<(), ExecutionError<S>> {
        let total_start = Instant::now();

        let steps = {
            let mut steps_lock = self.steps.lock().unwrap();
            // Grab the steps and component counts from within steps_lock, then let
            // steps_lock go. (Without this, clippy warns about steps_lock being
            // held across await points.)
            std::mem::take(&mut *steps_lock)
        };

        let step_infos: Vec<_> = steps
            .steps
            .iter()
            .enumerate()
            .map(|(index, step)| {
                let total_component_steps = steps
                    .component_counts
                    .get(&step.metadata_gen.component)
                    .expect("this component was added");
                step.metadata_gen.to_step_info(index, *total_component_steps)
            })
            .collect();

        let components = steps
            .component_counts
            .iter()
            .map(|(component, &total_component_steps)| StepComponentSummary {
                component: component.clone(),
                total_component_steps,
            })
            .collect();

        let mut steps_iter = steps.steps.into_iter().enumerate();

        // We need to handle the following separately:
        // * The first step
        // * Intermediate steps
        // * The last step

        let Some((index, first_step)) = steps_iter.next() else {
            // There are no steps defined.
            sender.send(Event::Step(StepEvent {
                total_elapsed: total_start.elapsed(),
                kind: StepEventKind::NoStepsDefined,
            })).await?;
            return Ok(());
        };

        let first_step_info = {
            let total_component_steps = steps
                .component_counts
                .get(&first_step.metadata_gen.component)
                .expect("this component was added");
            first_step
                .metadata_gen
                .into_step_info_with_metadata(index, *total_component_steps)
                .await
        };

        sender
            .send(Event::Step(StepEvent {
                total_elapsed: total_start.elapsed(),
                kind: StepEventKind::ExecutionStarted {
                    steps: step_infos,
                    components,
                    first_step: first_step_info.clone(),
                },
            }))
            .await?;

        let (mut step_res, mut reporter) = first_step
            .exec
            .execute(&self.log, total_start, first_step_info, sender.clone())
            .await?;

        // Now run all remaining steps.
        for (index, step) in steps_iter {
            let total_component_steps = steps
                .component_counts
                .get(&step.metadata_gen.component)
                .expect("this component was added");

            let step_info = step
                .metadata_gen
                .into_step_info_with_metadata(index, *total_component_steps)
                .await;

            reporter.next_step(step_res, &step_info).await?;

            (step_res, reporter) = step
                .exec
                .execute(&self.log, total_start, step_info, sender.clone())
                .await?;
        }

        // Finally, report the last step.
        reporter.last_step(step_res).await?;

        Ok(())
    }
}

#[derive_where(Default, Debug)]
struct Steps<'a, S: StepSpec> {
    steps: Vec<Step<'a, S>>,
    component_counts: LinearMap<S::Component, usize>,
}

// Note: have to be careful with lifetimes here because 'a is an invariant
// lifetime. If there are compile errors related to this, they're likely to be
// because 'a got mixed up with a covariant lifetime like 'engine.

/// Provides component context against which a step can be registered.
pub struct ComponentRegistrar<'engine, 'a: 'engine, S: StepSpec> {
    steps: &'engine Mutex<Steps<'a, S>>,
    component: S::Component,
}

impl<'engine, 'a, S: StepSpec> ComponentRegistrar<'engine, 'a, S> {
    /// Returns the component associated with this registrar.
    #[inline]
    pub fn component(&self) -> &S::Component {
        &self.component
    }

    /// Adds a new step corresponding to this component.
    pub fn new_step<F, Fut, T>(
        &self,
        id: S::StepId,
        description: impl Into<Cow<'static, str>>,
        step_fn: F,
    ) -> NewStep<'engine, 'a, S, T>
    where
        F: FnOnce(StepContext<S>) -> Fut + 'a,
        Fut: Future<Output = Result<StepResult<S, T>, S::Error>> + Send + 'a,
        T: Send + 'a,
    {
        let (sender, receiver) = oneshot::channel();

        let exec_fn = Box::new(move |cx: StepContext<S>| {
            let result = (step_fn)(cx);
            async move {
                match result.await {
                    Ok(val) => {
                        // Ignore errors if the receiver (the StepHandle) was dropped.
                        _ = sender.send(val.output);
                        Ok(val.outcome)
                    }
                    Err(error) => {
                        // This terminates progress.
                        Err(error)
                    }
                }
            }
            .boxed()
        });

        NewStep {
            steps: self.steps,
            component: self.component.clone(),
            id,
            description: description.into(),
            exec_fn: DebugIgnore(exec_fn),
            receiver,
            metadata_fn: None,
        }
    }
}

/// A new step that hasn't been registered by an execution engine yet.
///
/// Created by [`UpdateEngine::new_step`] or [`ComponentRegistrar::new_step`].
#[must_use = "call register() to register this step with the engine"]
#[derive(Debug)]
pub struct NewStep<'engine, 'a, S: StepSpec, T> {
    steps: &'engine Mutex<Steps<'a, S>>,
    component: S::Component,
    id: S::StepId,
    description: Cow<'static, str>,
    exec_fn: DebugIgnore<StepExecFn<'a, S>>,
    receiver: oneshot::Receiver<T>,
    metadata_fn: Option<DebugIgnore<StepMetadataFn<'a, S::StepMetadata>>>,
}

impl<'engine, 'a, S: StepSpec, T> NewStep<'engine, 'a, S, T> {
    /// Adds a metadata-generating function to the step.
    ///
    /// This function is expected to produce [`S::StepMetadata`]. The metadata
    /// function must be infallible, and will often just be synchronous code.
    pub fn with_metadata_fn<F, Fut>(mut self, f: F) -> Self
    where
        F: FnOnce() -> Fut + 'a,
        Fut: Future<Output = S::StepMetadata> + Send + 'a,
    {
        self.metadata_fn = Some(DebugIgnore(Box::new(|| (f)().boxed())));
        self
    }

    /// Registers the step with the engine.
    pub fn register(self) -> StepHandle<T> {
        let mut steps_lock = self.steps.lock().unwrap();
        let component_count = steps_lock
            .component_counts
            .entry(self.component.clone())
            .or_insert(0);
        let current_index = *component_count;
        *component_count += 1;

        let step = Step {
            metadata_gen: StepMetadataGen {
                id: self.id,
                component: self.component.clone(),
                component_index: current_index,
                description: self.description,
                metadata_fn: self.metadata_fn,
            },
            exec: StepExec { exec_fn: self.exec_fn },
        };
        steps_lock.steps.push(step);
        StepHandle { receiver: self.receiver }
    }
}

pin_project! {
    /// A future that resolves to a step's result.
    ///
    /// This handle can be used between steps to transfer data between steps.
    ///
    /// A `StepHandle` implements `Future`, so it can be awaited. To share the
    /// result across several dependent futures, use
    /// [`FutureExt::shared`](futures::FutureExt::shared).
    ///
    /// It is important that `StepHandle`s never be awaited outside the context
    /// of a step that comes afterwards -- doing so will cause an immediate
    /// deadlock.
    ///
    /// Dropping a `StepHandle` has no effect on whether the step itself runs.
    #[derive(Debug)]
    pub struct StepHandle<T> {
        #[pin]
        receiver: oneshot::Receiver<T>,
    }
}

// TODO: maybe export other receive methods from receiver, depending on need

impl<T> Future for StepHandle<T> {
    type Output = T;

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        let this = self.project();
        this.receiver
            .poll(cx)
            .map(|output| output.expect("update-engine never drops the sender"))
    }
}

/// The result of a step.
///
/// Returned by the callback passed to `register_step`.
pub struct StepResult<S: StepSpec, T> {
    /// The output of the step.
    pub output: T,

    /// The outcome associated with the step.
    ///
    /// This outcome is serializable.
    pub outcome: StepOutcome<S>,
}

impl<S: StepSpec, T> StepResult<S, T> {
    /// Creates a new `StepResult` corresponding to a successful output.
    pub fn success(output: T, metadata: S::CompletionMetadata) -> Self {
        Self { output, outcome: StepOutcome::Success { metadata } }
    }

    /// Creates a new `StepResult` corresponding to a successful output, with a
    /// warning attached.
    pub fn warning(
        output: T,
        metadata: S::CompletionMetadata,
        message: impl Into<Cow<'static, str>>,
    ) -> Self {
        Self {
            output,
            outcome: StepOutcome::Warning { metadata, message: message.into() },
        }
    }

    /// Creates a new `StepResult` corresponding to a skipped step, with a
    /// message attached.
    pub fn skipped(output: T, message: impl Into<Cow<'static, str>>) -> Self {
        Self {
            output,
            outcome: StepOutcome::Skipped { message: message.into() },
        }
    }
}

#[derive(Clone, Debug)]
pub struct StepContext<S: StepSpec> {
    progress_sender: mpsc::Sender<StepProgress<S>>,
}

impl<S: StepSpec> StepContext<S> {
    /// Sends a progress update to the update engine.
    #[inline]
    pub async fn send_progress(&self, progress: StepProgress<S>) {
        self.progress_sender
            .send(progress)
            .await
            .expect("our code always keeps the receiver open")
    }
}

/// A step consists of three components:
///
/// 1. Information about the step, including the component, ID, etc.
/// 2. Metadata about the step, generated in an async function. For example, for
///    a peer this can be the place it was downloaded from.
/// 3. The actual step function.
///
/// 1 and 2 are in StepMetadataGen, while 3 is in exec.
#[derive_where(Debug)]
struct Step<'a, S: StepSpec> {
    metadata_gen: StepMetadataGen<'a, S>,
    exec: StepExec<'a, S>,
}

#[derive_where(Debug)]
struct StepMetadataGen<'a, S: StepSpec> {
    id: S::StepId,
    component: S::Component,
    component_index: usize,
    description: Cow<'static, str>,
    metadata_fn: Option<DebugIgnore<StepMetadataFn<'a, S::StepMetadata>>>,
}

impl<'a, S: StepSpec> StepMetadataGen<'a, S> {
    fn to_step_info(
        &self,
        index: usize,
        total_component_steps: usize,
    ) -> StepInfo<S> {
        StepInfo {
            id: self.id.clone(),
            component: self.component.clone(),
            index,
            component_index: self.component_index,
            total_component_steps,
            description: self.description.clone(),
        }
    }

    async fn into_step_info_with_metadata(
        self,
        index: usize,
        total_component_steps: usize,
    ) -> StepInfoWithMetadata<S> {
        let info = self.to_step_info(index, total_component_steps);
        let metadata = match self.metadata_fn {
            None => None,
            Some(DebugIgnore(metadata_fn)) => {
                let metadata = (metadata_fn)().await;
                Some(metadata)
            }
        };

        StepInfoWithMetadata { info, metadata }
    }
}

#[derive_where(Debug)]
struct StepExec<'a, S: StepSpec> {
    exec_fn: DebugIgnore<StepExecFn<'a, S>>,
}

impl<'a, S: StepSpec> StepExec<'a, S> {
    async fn execute(
        self,
        log: &slog::Logger,
        total_start: Instant,
        step_info: StepInfoWithMetadata<S>,
        sender: mpsc::Sender<Event<S>>,
    ) -> Result<
        (Result<StepOutcome<S>, S::Error>, StepProgressReporter<S>),
        mpsc::error::SendError<Event<S>>,
    > {
        slog::debug!(
            log,
            "start executing step";
            "step component" => ?step_info.info.component,
            "step id" => ?step_info.info.id,
        );
        let (progress_sender, mut progress_receiver) = mpsc::channel(16);
        let cx = StepContext { progress_sender };

        let mut step_fut = (self.exec_fn.0)(cx);
        let mut reporter =
            StepProgressReporter::new(total_start, step_info, sender);

        let mut step_res = None;
        let mut progress_done = false;

        loop {
            tokio::select! {
                res = &mut step_fut, if step_res.is_none() => {
                    step_res = Some(res);
                }

                progress = progress_receiver.recv(), if !progress_done => {
                    match progress {
                        Some(progress) => {
                            reporter.handle_progress(progress).await?;
                        }
                        None => {
                            // The progress receiver is complete.
                            progress_done = true;
                        }
                    }
                }

                else => break,
            }
        }

        // Return the result -- the caller is responsible for handling events.
        let step_res = step_res.expect("can only get here if res is Some");
        Ok((step_res, reporter))
    }
}

type StepMetadataFn<'a, M> = Box<dyn FnOnce() -> BoxFuture<'a, M> + 'a>;

type StepExecFn<'a, S> = Box<
    dyn FnOnce(
            StepContext<S>,
        )
            -> BoxFuture<'a, Result<StepOutcome<S>, <S as StepSpec>::Error>>
        + 'a,
>;

struct StepProgressReporter<S: StepSpec> {
    total_start: Instant,
    step_info: StepInfoWithMetadata<S>,
    step_start: Instant,
    attempt: usize,
    attempt_start: Instant,
    sender: mpsc::Sender<Event<S>>,
}

impl<S: StepSpec> StepProgressReporter<S> {
    fn new(
        total_start: Instant,
        step_info: StepInfoWithMetadata<S>,
        sender: mpsc::Sender<Event<S>>,
    ) -> Self {
        let step_start = Instant::now();
        Self {
            total_start,
            step_info,
            step_start,
            attempt: 1,
            // It's slightly nicer for step_start and attempt_start to be exactly the same.
            attempt_start: step_start,
            sender,
        }
    }

    async fn handle_progress(
        &mut self,
        progress: StepProgress<S>,
    ) -> Result<(), mpsc::error::SendError<Event<S>>> {
        match progress {
            StepProgress::Progress { progress, metadata } => {
                // Send the progress to the sender.
                self.sender
                    .send(Event::Progress(ProgressEvent {
                        total_elapsed: self.total_start.elapsed(),
                        kind: ProgressEventKind::Progress {
                            step: self.step_info.clone(),
                            attempt: self.attempt,
                            progress,
                            metadata,
                            step_elapsed: self.step_start.elapsed(),
                            attempt_elapsed: self.attempt_start.elapsed(),
                        },
                    }))
                    .await
            }
            StepProgress::Reset { metadata, message } => {
                // Send a progress reset message, but do not reset the attempt.
                self.sender
                    .send(Event::Step(StepEvent {
                        total_elapsed: self.total_start.elapsed(),
                        kind: StepEventKind::ProgressReset {
                            step: self.step_info.clone(),
                            attempt: self.attempt,
                            metadata,
                            step_elapsed: self.step_start.elapsed(),
                            attempt_elapsed: self.attempt_start.elapsed(),
                            message,
                        },
                    }))
                    .await
            }
            StepProgress::Retry { message } => {
                // Retry this step.
                self.attempt += 1;
                let attempt_elapsed = self.attempt_start.elapsed();
                self.attempt_start = Instant::now();

                // Send the retry message.
                self.sender
                    .send(Event::Step(StepEvent {
                        total_elapsed: self.total_start.elapsed(),
                        kind: StepEventKind::AttemptRetry {
                            step: self.step_info.clone(),
                            next_attempt: self.attempt,
                            step_elapsed: self.step_start.elapsed(),
                            attempt_elapsed,
                            message,
                        },
                    }))
                    .await
            }
        }
    }

    async fn next_step(
        self,
        step_res: Result<StepOutcome<S>, S::Error>,
        next_step_info: &StepInfoWithMetadata<S>,
    ) -> Result<(), ExecutionError<S>> {
        match step_res {
            Ok(outcome) => {
                self.sender
                    .send(Event::Step(StepEvent {
                        total_elapsed: self.total_start.elapsed(),
                        kind: StepEventKind::StepCompleted {
                            step: self.step_info,
                            attempt: self.attempt,
                            outcome,
                            next_step: next_step_info.clone(),
                            step_elapsed: self.step_start.elapsed(),
                            attempt_elapsed: self.attempt_start.elapsed(),
                        },
                    }))
                    .await?;
                Ok(())
            }
            Err(error) => {
                let component = self.step_info.info.component.clone();
                let id = self.step_info.info.id.clone();
                self.send_error(&error).await?;
                Err(ExecutionError::StepFailed { component, id, error })
            }
        }
    }

    async fn last_step(
        self,
        step_res: Result<StepOutcome<S>, S::Error>,
    ) -> Result<(), ExecutionError<S>> {
        match step_res {
            Ok(outcome) => {
                self.sender
                    .send(Event::Step(StepEvent {
                        total_elapsed: self.total_start.elapsed(),
                        kind: StepEventKind::ExecutionCompleted {
                            last_step: self.step_info,
                            last_attempt: self.attempt,
                            last_outcome: outcome,
                            step_elapsed: self.step_start.elapsed(),
                            attempt_elapsed: self.attempt_start.elapsed(),
                        },
                    }))
                    .await?;
                Ok(())
            }
            Err(error) => {
                let component = self.step_info.info.component.clone();
                let id = self.step_info.info.id.clone();
                self.send_error(&error).await?;
                Err(ExecutionError::StepFailed { component, id, error })
            }
        }
    }

    async fn send_error(
        self,
        error: &S::Error,
    ) -> Result<(), mpsc::error::SendError<Event<S>>> {
        let error = error.as_error();
        let message = error.to_string();

        let mut current = error;
        let mut causes = vec![];
        while let Some(source) = current.source() {
            causes.push(source.to_string());
            current = source;
        }

        self.sender
            .send(Event::Step(StepEvent {
                total_elapsed: self.total_start.elapsed(),
                kind: StepEventKind::ExecutionFailed {
                    failed_step: self.step_info,
                    total_attempts: self.attempt,
                    step_elapsed: self.step_start.elapsed(),
                    attempt_elapsed: self.attempt_start.elapsed(),
                    message,
                    causes,
                },
            }))
            .await
    }
}

#[cfg(test)]
mod tests {
    use anyhow::bail;
    use omicron_test_utils::dev::test_setup_log;
    use tokio_stream::wrappers::ReceiverStream;

    use crate::test_utils::TestSpec;

    use super::*;

    #[tokio::test]
    async fn error_exits_early() {
        let logctx = test_setup_log("error_exits_early");

        let mut step_1_run = false;
        let mut step_2_run = false;
        let mut step_3_run = false;

        let engine: UpdateEngine<TestSpec> = UpdateEngine::new(&logctx.log);

        engine
            .new_step("foo".to_owned(), 0, "Step 1", |_| async {
                step_1_run = true;
                Ok(StepResult::success((), serde_json::Value::Null))
            })
            .register();

        engine
            .new_step::<_, _, ()>("bar".to_owned(), 0, "Step 2", |_| async {
                step_2_run = true;
                bail!("example failed")
            })
            .register();

        engine
            .new_step("baz".to_owned(), 0, "Step 3", |_| async {
                step_3_run = true;
                Ok(StepResult::success((), serde_json::Value::Null))
            })
            .register();

        // Make a buffer big enough that the engine can never fill it up.
        let (sender, receiver) = mpsc::channel(512);

        engine
            .execute(sender)
            .await
            .expect_err("step 2 failed so we should see an error here");

        let events: Vec<_> = ReceiverStream::new(receiver).collect().await;
        let last_event = events.last().unwrap();
        match last_event {
            Event::Step(step_event) => {
                assert!(
                    matches!(
                        &step_event.kind,
                        StepEventKind::ExecutionFailed { failed_step, message, .. }
                        if failed_step.info.component == "bar"
                        && message == "example failed"
                    ),
                    "event didn't match: {last_event:?}"
                )
            }
            _ => panic!("unexpected event: {last_event:?}"),
        }

        assert!(step_1_run, "Step 1 was run");
        assert!(step_2_run, "Step 2 was run");
        assert!(!step_3_run, "Step 3 was not run");
    }
}
