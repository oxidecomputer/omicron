// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use std::{
    borrow::Cow,
    fmt,
    ops::ControlFlow,
    pin::Pin,
    sync::{Arc, Mutex},
    task::Poll,
};

use cancel_safe_futures::coop_cancel;
use debug_ignore::DebugIgnore;
use derive_where::derive_where;
use futures::{future::BoxFuture, prelude::*};
use linear_map::LinearMap;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{mpsc, oneshot},
    time::Instant,
};
use uuid::Uuid;

use crate::{
    errors::ExecutionError,
    events::{
        Event, ProgressEvent, ProgressEventKind, StepComponentSummary,
        StepEvent, StepEventKind, StepInfo, StepInfoWithMetadata, StepOutcome,
        StepProgress,
    },
    AsError, CompletionContext, MetadataContext, NestedSpec, StepContext,
    StepContextPayload, StepHandle, StepSpec,
};

/// An identifier for a particular engine execution.
///
/// All events coming from an execution have the same engine ID. Nested engines
/// have their own ID.
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Deserialize,
    Serialize,
    JsonSchema,
)]
#[serde(transparent)]
pub struct ExecutionId(pub Uuid);

impl fmt::Display for ExecutionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive_where(Debug)]
pub struct UpdateEngine<'a, S: StepSpec> {
    // TODO: for now, this is a sequential series of steps. This can potentially
    // be a graph in the future.
    log: slog::Logger,
    execution_id: ExecutionId,
    sender: EngineSender<S>,

    // This is set to None in Self::execute.
    canceler: Option<coop_cancel::Canceler<String>>,
    cancel_receiver: coop_cancel::Receiver<String>,

    // This is a mutex to allow borrows to steps to be held by both
    // ComponentRegistrar and NewStep at the same time. (This could also be a
    // `RefCell` if a `Send` bound isn't required.)
    //
    // There is an alternative way to do this that doesn't use a mutex but
    // involves no less than three lifetime parameters, which is excessive.
    steps: Mutex<Steps<'a, S>>,
}

impl<'a, S: StepSpec + 'a> UpdateEngine<'a, S> {
    /// Creates a new `UpdateEngine`.
    pub fn new(log: &slog::Logger) -> (Self, mpsc::Receiver<Event<S>>) {
        // Set a large enough buffer that it filling up isn't an actual problem
        // outside of something going horribly wrong.
        let (sender, receiver) = mpsc::channel(512);
        let sender = Arc::new(DefaultSender { sender });
        let ret = Self::new_impl(log, EngineSender { sender });
        (ret, receiver)
    }

    // See the comment on `StepContext::with_nested_engine` for why this is
    // necessary.``
    pub(crate) fn new_nested<S2: StepSpec>(
        log: &slog::Logger,
        sender: mpsc::Sender<StepContextPayload<S2>>,
    ) -> Self {
        let sender = Arc::new(NestedSender { sender });
        Self::new_impl(log, EngineSender { sender })
    }

    fn new_impl(log: &slog::Logger, sender: EngineSender<S>) -> Self {
        let execution_id = ExecutionId(Uuid::new_v4());
        let (canceler, cancel_receiver) = coop_cancel::new_pair();
        Self {
            log: log.new(slog::o!(
                "component" => "UpdateEngine",
                "execution_id" => format!("{execution_id}"),
            )),
            execution_id: ExecutionId(Uuid::new_v4()),
            sender,
            canceler: Some(canceler),
            cancel_receiver,
            steps: Default::default(),
        }
    }

    /// Returns the ID for this execution.
    ///
    /// All events coming from this engine will have this ID associated with
    /// them.
    pub fn execution_id(&self) -> ExecutionId {
        self.execution_id
    }

    /// Adds a new step corresponding to the given component.
    ///
    /// # Notes
    ///
    /// The step will be considered to keep running until both the future
    /// completes and the `StepContext` is dropped. In normal use, both happen
    /// at the same time. However, it is technically possible to make the
    /// `StepContext` escape the future.
    ///
    /// (Ideally, this would be prevented by making the function take a `&mut
    /// StepContext`, but there are limitations in stable Rust which make this
    /// impossible to achieve.)
    pub fn new_step<F, Fut, T>(
        &self,
        component: S::Component,
        id: S::StepId,
        description: impl Into<Cow<'static, str>>,
        step_fn: F,
    ) -> NewStep<'_, 'a, S, T>
    where
        F: FnOnce(StepContext<S>) -> Fut + Send + 'a,
        Fut: Future<Output = Result<StepResult<T, S>, S::Error>> + Send + 'a,
        T: Send + 'a,
    {
        self.for_component(component).new_step(id, description, step_fn)
    }

    /// Creates a [`ComponentRegistrar`] that defines steps within the context
    /// of a component.
    ///
    /// It is often useful to define similar steps across multiple components. A
    /// `ComponentRegistrar` provides an easy way to do so.
    pub fn for_component(
        &self,
        component: S::Component,
    ) -> ComponentRegistrar<'_, 'a, S> {
        ComponentRegistrar { steps: &self.steps, component }
    }

    /// Creates and returns an abort handle for this engine.
    ///
    /// An abort handle can be used to forcibly cancel update engine executions.
    pub fn abort_handle(&self) -> AbortHandle {
        AbortHandle {
            canceler: self
                .canceler
                .as_ref()
                .expect("abort_sender should always be present")
                .clone(),
        }
    }

    /// Executes the engine.
    ///
    /// This returns an `ExecutionHandle`, which needs to be awaited on to drive
    /// the engine forward.
    pub fn execute(mut self) -> ExecutionHandle<'a, S> {
        let canceler = self
            .canceler
            .take()
            .expect("execute is the only function which does this");
        let abort_handle = AbortHandle { canceler };

        let engine_fut = self.execute_impl().boxed();

        ExecutionHandle { engine_fut: DebugIgnore(engine_fut), abort_handle }
    }

    async fn execute_impl(
        mut self,
    ) -> Result<CompletionContext<S>, ExecutionError<S>> {
        let mut event_index = 0;
        let next_event_index = || {
            event_index += 1;
            event_index - 1
        };
        let mut exec_cx = ExecutionContext::new(
            self.execution_id,
            next_event_index,
            self.sender.clone(),
        );

        let steps = {
            let mut steps_lock = self.steps.lock().unwrap();
            // Grab the steps and component counts from within steps_lock, then
            // let steps_lock go. (Without this, clippy warns about steps_lock
            // being held across await points.)
            //
            // There are no concurrency concerns here because `execute` consumes
            // `self`, and is the only piece of code that has access to the
            // mutex (`self.steps` is a `Mutex<T>`, not an `Arc<Mutex<T>>`!)
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
            self.sender
                .send(Event::Step(StepEvent {
                    spec: S::schema_name(),
                    execution_id: self.execution_id,
                    event_index: (exec_cx.next_event_index)(),
                    total_elapsed: exec_cx.total_start.elapsed(),
                    kind: StepEventKind::NoStepsDefined,
                }))
                .await?;
            return Ok(CompletionContext::new());
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

        let event = Event::Step(StepEvent {
            spec: S::schema_name(),
            execution_id: self.execution_id,
            event_index: (exec_cx.next_event_index)(),
            total_elapsed: exec_cx.total_start.elapsed(),
            kind: StepEventKind::ExecutionStarted {
                steps: step_infos,
                components,
                first_step: first_step_info.clone(),
            },
        });

        self.sender.send(event).await?;

        let step_exec_cx = exec_cx.create(first_step_info);

        let (mut step_res, mut reporter) = first_step
            .exec
            .execute(&self.log, step_exec_cx, &mut self.cancel_receiver)
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
            let next_step = reporter.next_step(step_res, &step_info);
            next_step.await?;

            let step_exec_cx = exec_cx.create(step_info);

            (step_res, reporter) = step
                .exec
                .execute(&self.log, step_exec_cx, &mut self.cancel_receiver)
                .await?;
        }

        // Finally, report the last step.
        reporter.last_step(step_res).await?;

        Ok(CompletionContext::new())
    }
}

/// Abstraction used to send events to whatever receiver is interested in them.
///
/// # Why is this type so weird?
///
/// `EngineSender` is a wrapper around a cloneable trait object. Why do we need
/// that?
///
/// `SenderImpl` has two implementations:
///
/// 1. `DefaultSender`, which is a wrapper around an `mpsc::Sender<Event<S>>`.
///    This is used when the receiver is user code.
/// 2. `NestedSender`, which is a more complex wrapper around an
///    `mpsc::Sender<StepContextPayload<S>>`.
///
/// You might imagine that we could just have `EngineSender` be an enum with
/// these two variants. But we actually want `NestedSender<S>` to implement
/// `SenderImpl<S>` for *any* StepSpec, not just `S`, to allow nested engines to
/// be a different StepSpec than the outer engine.
///
/// In other words, `NestedSender` doesn't just represent a single
/// `mpsc::Sender<StepContextPayload<S>>`, it represents the universe of all
/// possible StepSpecs S. This is an infinite number of variants, and requires a
/// trait object to represent.
#[derive_where(Clone, Debug)]
struct EngineSender<S: StepSpec> {
    sender: Arc<dyn SenderImpl<S>>,
}

impl<S: StepSpec> EngineSender<S> {
    async fn send(&self, event: Event<S>) -> Result<(), ExecutionError<S>> {
        self.sender.send(event).await
    }
}

trait SenderImpl<S: StepSpec>: Send + Sync + fmt::Debug {
    fn send(
        &self,
        event: Event<S>,
    ) -> BoxFuture<'_, Result<(), ExecutionError<S>>>;
}

#[derive_where(Debug)]
struct DefaultSender<S: StepSpec> {
    sender: mpsc::Sender<Event<S>>,
}

impl<S: StepSpec> SenderImpl<S> for DefaultSender<S> {
    fn send(
        &self,
        event: Event<S>,
    ) -> BoxFuture<'_, Result<(), ExecutionError<S>>> {
        self.sender.send(event).map_err(|error| error.into()).boxed()
    }
}

#[derive_where(Debug)]
struct NestedSender<S: StepSpec> {
    sender: mpsc::Sender<StepContextPayload<S>>,
}

// Note that NestedSender<S> implements SenderImpl<S2> for any S2: StepSpec.
// That is to allow nested engines to implement arbitrary StepSpecs.
impl<S: StepSpec, S2: StepSpec> SenderImpl<S2> for NestedSender<S> {
    fn send(
        &self,
        event: Event<S2>,
    ) -> BoxFuture<'_, Result<(), ExecutionError<S2>>> {
        let now = Instant::now();
        async move {
            let (done, done_rx) = oneshot::channel();
            self.sender
                .send(StepContextPayload::NestedSingle {
                    now,
                    event: event.into_generic(),
                    done,
                })
                .await
                .expect("our code always keeps payload_receiver open");
            _ = done_rx.await;
            Ok(())
        }
        .boxed()
    }
}

/// A join handle for an UpdateEngine.
///
/// This handle should be awaited to drive and obtain the result of an execution.
#[derive(Debug)]
#[must_use = "ExecutionHandle does nothing unless polled"]
pub struct ExecutionHandle<'a, S: StepSpec> {
    engine_fut: DebugIgnore<
        BoxFuture<'a, Result<CompletionContext<S>, ExecutionError<S>>>,
    >,
    abort_handle: AbortHandle,
}

impl<'a, S: StepSpec> ExecutionHandle<'a, S> {
    /// Aborts this engine execution with a message.
    ///
    /// This sends the message immediately, and returns a future that can be
    /// optionally waited against to block until the abort is processed.
    ///
    /// If this engine is still running, it is aborted at the next await point.
    /// The engine sends an `ExecutionAborted` message over the wire, and an
    /// `ExecutionError::Aborted` is returned.
    ///
    /// Returns `Err(message)` if the engine has already completed execution.
    pub fn abort(
        &self,
        message: impl Into<String>,
    ) -> Result<AbortWaiter, String> {
        self.abort_handle.abort(message.into())
    }

    /// Creates and returns an abort handle for this engine.
    ///
    /// An abort handle can be used to forcibly cancel update engine executions.
    pub fn abort_handle(&self) -> AbortHandle {
        self.abort_handle.clone()
    }
}

impl<'a, S: StepSpec> Future for ExecutionHandle<'a, S> {
    type Output = Result<CompletionContext<S>, ExecutionError<S>>;

    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.engine_fut.0.as_mut().poll(cx)
    }
}

/// An abort handle, used to forcibly cancel update engine executions.
#[derive(Clone, Debug)]
pub struct AbortHandle {
    canceler: coop_cancel::Canceler<String>,
}

impl AbortHandle {
    /// Aborts this engine execution with a message.
    ///
    /// This sends the message immediately, and returns a future that can be
    /// optionally waited against to block until the abort is processed.
    ///
    /// If this engine is still running, it is aborted at the next await point.
    /// The engine sends an `ExecutionAborted` message over the wire, and an
    /// `ExecutionError::Aborted` is returned.
    ///
    /// Returns `Err(message)` if the engine has already completed execution.
    pub fn abort(
        &self,
        message: impl Into<String>,
    ) -> Result<AbortWaiter, String> {
        let waiter = self.canceler.cancel(message.into())?;
        Ok(AbortWaiter { waiter })
    }
}

/// A future which can be used to optionally block until an abort message is
/// processed.
///
/// Dropping this future does not cancel the abort.
#[derive(Debug)]
pub struct AbortWaiter {
    waiter: coop_cancel::Waiter<String>,
}

impl Future for AbortWaiter {
    type Output = ();

    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        self.waiter.poll_unpin(cx)
    }
}

#[derive_where(Default, Debug)]
struct Steps<'a, S: StepSpec> {
    steps: Vec<Step<'a, S>>,

    // This is a `LinearMap` and not a `HashMap`/`BTreeMap` because we don't
    // want to impose a `Hash` or `Ord` restriction on `S::Component`. In
    // particular, we want to support `S::Component` being a generic
    // `serde_json::Value`, which doesn't implement `Hash` or `Ord` but does
    // implement `Eq`.
    component_counts: LinearMap<S::Component, usize>,
}

// Note: have to be careful with lifetimes here because 'a is an invariant
// lifetime. If there are compile errors related to this, they're likely to be
// because 'a got mixed up with a covariant lifetime like 'engine.

/// Provides component context against which a step can be registered.
pub struct ComponentRegistrar<'engine, 'a, S: StepSpec> {
    steps: &'engine Mutex<Steps<'a, S>>,
    component: S::Component,
}

impl<'engine, 'a, S: StepSpec> ComponentRegistrar<'engine, 'a, S> {
    /// Returns the component associated with this registrar.
    #[inline]
    pub fn component(&self) -> &S::Component {
        &self.component
    }

    /// Adds a new step corresponding to the component associated with the
    /// registrar.
    ///
    /// # Notes
    ///
    /// The step will be considered to keep running until both the future
    /// completes and the `StepContext` is dropped. In normal use, both happen
    /// at the same time. However, it is technically possible to make the
    /// `StepContext` escape the future.
    ///
    /// (Ideally, this would be prevented by making the function take a `&mut
    /// StepContext`, but there are limitations in stable Rust which make this
    /// impossible to achieve.)
    pub fn new_step<F, Fut, T>(
        &self,
        id: S::StepId,
        description: impl Into<Cow<'static, str>>,
        step_fn: F,
    ) -> NewStep<'engine, 'a, S, T>
    where
        F: FnOnce(StepContext<S>) -> Fut + Send + 'a,
        Fut: Future<Output = Result<StepResult<T, S>, S::Error>> + Send + 'a,
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
    metadata_fn: Option<DebugIgnore<StepMetadataFn<'a, S>>>,
}

impl<'engine, 'a, S: StepSpec, T> NewStep<'engine, 'a, S, T> {
    /// Adds a metadata-generating function to the step.
    ///
    /// This function is expected to produce
    /// [`S::StepMetadata`](StepSpec::StepMetadata). The metadata function must
    /// be infallible, and will often just be synchronous code.
    pub fn with_metadata_fn<F, Fut>(mut self, f: F) -> Self
    where
        F: FnOnce(MetadataContext<S>) -> Fut + Send + 'a,
        Fut: Future<Output = S::StepMetadata> + Send + 'a,
    {
        self.metadata_fn = Some(DebugIgnore(Box::new(|cx| (f)(cx).boxed())));
        self
    }

    /// Registers the step with the engine.
    pub fn register(self) -> StepHandle<T, S> {
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
        StepHandle::new(self.receiver)
    }
}

/// The result of a step.
///
/// Returned by the callback passed to `register_step`.
#[derive_where(Debug; T: std::fmt::Debug)]
#[must_use = "StepResult must be used"]
pub struct StepResult<T, S: StepSpec> {
    /// The output of the step.
    pub output: T,

    /// The outcome associated with the step.
    ///
    /// This outcome is serializable.
    pub outcome: StepOutcome<S>,
}

impl<T, S: StepSpec> StepResult<T, S> {
    /// Maps a `StepResult<T, S>` to `StepResult<U, S>` by applying a function
    /// to the contained `output` value, leaving the `outcome` untouched.
    pub fn map<U, F>(self, op: F) -> StepResult<U, S>
    where
        F: FnOnce(T) -> U,
    {
        StepResult { output: op(self.output), outcome: self.outcome }
    }
}

/// A success result produced by a step.
#[derive_where(Debug; T: std::fmt::Debug)]
#[must_use = "StepSuccess must be used"]
pub struct StepSuccess<T, S: StepSpec> {
    /// The output of the step.
    pub output: T,

    /// An optional message associated with this result.
    pub message: Option<Cow<'static, str>>,

    /// Optional metadata associated with this step.
    pub metadata: Option<S::CompletionMetadata>,
}

impl<T, S: StepSpec> StepSuccess<T, S> {
    /// Creates a new `StepSuccess`.
    pub fn new(output: T) -> Self {
        Self { output, metadata: None, message: None }
    }

    /// Adds a message to this step.
    pub fn with_message(
        mut self,
        message: impl Into<Cow<'static, str>>,
    ) -> Self {
        self.message = Some(message.into());
        self
    }

    /// Adds metadata to this step.
    pub fn with_metadata(mut self, metadata: S::CompletionMetadata) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Creates a `StepResult` from this `StepSuccess`.
    pub fn build(self) -> StepResult<T, S> {
        StepResult {
            output: self.output,
            outcome: StepOutcome::Success {
                message: self.message,
                metadata: self.metadata,
            },
        }
    }
}

impl<T, S: StepSpec> From<StepSuccess<T, S>>
    for Result<StepResult<T, S>, S::Error>
{
    fn from(value: StepSuccess<T, S>) -> Self {
        Ok(value.build())
    }
}

#[derive_where(Debug; T: std::fmt::Debug)]
#[must_use = "StepWarning must be used"]
pub struct StepWarning<T, S: StepSpec> {
    /// The output of the step.
    pub output: T,

    /// A message associated with this result.
    pub message: Cow<'static, str>,

    /// Optional metadata associated with this step.
    pub metadata: Option<S::CompletionMetadata>,
}

impl<T, S: StepSpec> StepWarning<T, S> {
    /// Creates a new `StepWarning`.
    pub fn new(output: T, message: impl Into<Cow<'static, str>>) -> Self {
        Self { output, message: message.into(), metadata: None }
    }

    /// Adds metadata to this step.
    pub fn with_metadata(mut self, metadata: S::CompletionMetadata) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Creates a `StepResult` from this `StepSuccess`.
    pub fn build(self) -> StepResult<T, S> {
        StepResult {
            output: self.output,
            outcome: StepOutcome::Warning {
                message: self.message,
                metadata: self.metadata,
            },
        }
    }
}

impl<T, S: StepSpec> From<StepWarning<T, S>>
    for Result<StepResult<T, S>, S::Error>
{
    fn from(value: StepWarning<T, S>) -> Self {
        Ok(value.build())
    }
}

#[derive_where(Debug; T: std::fmt::Debug)]
#[must_use = "StepSkipped must be used"]
pub struct StepSkipped<T, S: StepSpec> {
    /// The output of the step.
    pub output: T,

    /// A message associated with this step.
    pub message: Cow<'static, str>,

    /// Optional metadata associated with this step.
    pub metadata: Option<S::SkippedMetadata>,
}

impl<T, S: StepSpec> StepSkipped<T, S> {
    /// Creates a new `StepSuccess`.
    pub fn new(output: T, message: impl Into<Cow<'static, str>>) -> Self {
        Self { output, message: message.into(), metadata: None }
    }

    /// Adds metadata to this step.
    pub fn with_metadata(mut self, metadata: S::SkippedMetadata) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Creates a `StepResult` from this `StepSuccess`.
    pub fn build(self) -> StepResult<T, S> {
        StepResult {
            output: self.output,
            outcome: StepOutcome::Skipped {
                message: self.message,
                metadata: self.metadata,
            },
        }
    }
}

impl<T, S: StepSpec> From<StepSkipped<T, S>>
    for Result<StepResult<T, S>, S::Error>
{
    fn from(value: StepSkipped<T, S>) -> Self {
        Ok(value.build())
    }
}

/// A step consists of three components:
///
/// 1. Information about the step, including the component, ID, etc.
/// 2. Metadata about the step, generated in an async function. For example, for
///    this can be a hash of an artifact, or an address it was downloaded from.
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
    metadata_fn: Option<DebugIgnore<StepMetadataFn<'a, S>>>,
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
                let cx = MetadataContext::new();
                let metadata_fut = (metadata_fn)(cx);
                let metadata = metadata_fut.await;
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
    async fn execute<F: FnMut() -> usize>(
        self,
        log: &slog::Logger,
        step_exec_cx: StepExecutionContext<S, F>,
        cancel_receiver: &mut coop_cancel::Receiver<String>,
    ) -> Result<
        (Result<StepOutcome<S>, S::Error>, StepProgressReporter<S, F>),
        ExecutionError<S>,
    > {
        slog::debug!(
            log,
            "start executing step";
            "step component" => ?step_exec_cx.step_info.info.component,
            "step id" => ?step_exec_cx.step_info.info.id,
        );
        let (payload_sender, mut payload_receiver) = mpsc::channel(16);
        let cx = StepContext::new(log, payload_sender);

        let mut step_fut = (self.exec_fn.0)(cx);
        let mut reporter = StepProgressReporter::new(step_exec_cx);

        let mut step_res = None;
        let mut payload_done = false;

        loop {
            // This is the main execution select loop. We break it up into two
            // portions:
            //
            // 1. The inner select, which is the meat of the engine. It consists
            //    of driving the step and the payload receiver forward.
            //
            // 2. The outer select, which consists of selecting over the inner
            //    select and the abort receiver.
            //
            // The two selects cannot be combined! That's because the else block
            // of the inner select only applies to the step and payload
            // receivers. We do not want to wait for the abort receiver to exit
            // before exiting the loop.
            let inner_select = async {
                tokio::select! {
                    res = &mut step_fut, if step_res.is_none() => {
                        step_res = Some(res);
                        Ok(ControlFlow::Continue(()))
                    }

                    // Note: payload_receiver is always kept open while step_fut
                    // is being driven. It is only dropped before completion if
                    // the step is aborted, in which case step_fut is also
                    // cancelled without being driven further. A bunch of
                    // expects with "our code always keeps payload_receiver
                    // open" rely on this.
                    //
                    // If we ever move the payload receiver to another task so
                    // it runs in parallel, this situation would have to be
                    // handled with care.
                    payload = payload_receiver.recv(), if !payload_done => {
                        match payload {
                            Some(payload) => {
                                reporter.handle_payload(payload).await?;
                            }
                            None => {
                                // The payload receiver is complete.
                                payload_done = true;
                            }
                        }
                        Ok(ControlFlow::Continue(()))
                    }

                    else => Ok(ControlFlow::Break(())),
                }
            };

            // This is the outer select.
            tokio::select! {
                ret = inner_select => {
                    match ret {
                        Ok(op) => {
                            if op.is_break() {
                                break;
                            }
                        }
                        Err(error) => {
                            return Err(error);
                        }
                    }
                }

                Some(message) = cancel_receiver.recv() => {
                    return Err(reporter.handle_abort(message).await);
                }
            }
        }

        // Return the result -- the caller is responsible for handling events.
        let step_res = step_res.expect("can only get here if res is Some");
        Ok((step_res, reporter))
    }
}

#[derive_where(Debug)]
struct ExecutionContext<S: StepSpec, F> {
    execution_id: ExecutionId,
    next_event_index: DebugIgnore<F>,
    total_start: Instant,
    sender: EngineSender<S>,
}

impl<S: StepSpec, F> ExecutionContext<S, F> {
    fn new(
        execution_id: ExecutionId,
        next_event_index: F,
        sender: EngineSender<S>,
    ) -> Self {
        let total_start = Instant::now();
        Self {
            execution_id,
            next_event_index: DebugIgnore(next_event_index),
            total_start,
            sender,
        }
    }

    fn create(
        &mut self,
        step_info: StepInfoWithMetadata<S>,
    ) -> StepExecutionContext<S, &mut F> {
        StepExecutionContext {
            execution_id: self.execution_id,
            next_event_index: DebugIgnore(&mut self.next_event_index.0),
            total_start: self.total_start,
            step_info,
            sender: self.sender.clone(),
        }
    }
}

#[derive_where(Debug)]
struct StepExecutionContext<S: StepSpec, F> {
    execution_id: ExecutionId,
    next_event_index: DebugIgnore<F>,
    total_start: Instant,
    step_info: StepInfoWithMetadata<S>,
    sender: EngineSender<S>,
}

type StepMetadataFn<'a, S> = Box<
    dyn FnOnce(
            MetadataContext<S>,
        ) -> BoxFuture<'a, <S as StepSpec>::StepMetadata>
        + Send
        + 'a,
>;

/// NOTE: Ideally this would take `&mut StepContext<S>`, so that it can't get
/// squirreled away by a step's function. However, that quickly runs into [this
/// issue in
/// Rust](https://users.rust-lang.org/t/passing-self-to-callback-returning-future-vs-lifetimes/53352).
///
/// It is probably possible to use unsafe code here, though that opens up its
/// own can of worms.
type StepExecFn<'a, S> = Box<
    dyn FnOnce(
            StepContext<S>,
        )
            -> BoxFuture<'a, Result<StepOutcome<S>, <S as StepSpec>::Error>>
        + Send
        + 'a,
>;

struct StepProgressReporter<S: StepSpec, F> {
    execution_id: ExecutionId,
    next_event_index: F,
    total_start: Instant,
    step_info: StepInfoWithMetadata<S>,
    step_start: Instant,
    attempt: usize,
    attempt_start: Instant,
    sender: EngineSender<S>,
}

impl<S: StepSpec, F: FnMut() -> usize> StepProgressReporter<S, F> {
    fn new(step_exec_cx: StepExecutionContext<S, F>) -> Self {
        let step_start = Instant::now();
        Self {
            execution_id: step_exec_cx.execution_id,
            next_event_index: step_exec_cx.next_event_index.0,
            total_start: step_exec_cx.total_start,
            step_info: step_exec_cx.step_info,
            step_start,
            attempt: 1,
            // It's slightly nicer for step_start and attempt_start to be exactly the same.
            attempt_start: step_start,
            sender: step_exec_cx.sender,
        }
    }

    async fn handle_payload(
        &mut self,
        payload: StepContextPayload<S>,
    ) -> Result<(), ExecutionError<S>> {
        match payload {
            StepContextPayload::Progress { now, progress, done } => {
                self.handle_progress(now, progress).await?;
                std::mem::drop(done);
            }
            StepContextPayload::NestedSingle { now, event, done } => {
                self.handle_nested(now, event).await?;
                std::mem::drop(done);
            }
            StepContextPayload::Nested { now, event } => {
                self.handle_nested(now, event).await?;
            }
            StepContextPayload::Sync { done } => {
                std::mem::drop(done);
            }
        }

        Ok(())
    }

    async fn handle_progress(
        &mut self,
        now: Instant,
        progress: StepProgress<S>,
    ) -> Result<(), ExecutionError<S>> {
        match progress {
            StepProgress::Progress { progress, metadata } => {
                // Send the progress to the sender.
                self.sender
                    .send(Event::Progress(ProgressEvent {
                        spec: S::schema_name(),
                        execution_id: self.execution_id,
                        total_elapsed: now - self.total_start,
                        kind: ProgressEventKind::Progress {
                            step: self.step_info.clone(),
                            attempt: self.attempt,
                            progress,
                            metadata,
                            step_elapsed: now - self.step_start,
                            attempt_elapsed: now - self.attempt_start,
                        },
                    }))
                    .await
            }
            StepProgress::Reset { metadata, message } => {
                // Send a progress reset message, but do not reset the attempt.
                self.sender
                    .send(Event::Step(StepEvent {
                        spec: S::schema_name(),
                        execution_id: self.execution_id,
                        event_index: (self.next_event_index)(),
                        total_elapsed: now - self.total_start,
                        kind: StepEventKind::ProgressReset {
                            step: self.step_info.clone(),
                            attempt: self.attempt,
                            metadata,
                            step_elapsed: now - self.step_start,
                            attempt_elapsed: now - self.attempt_start,
                            message,
                        },
                    }))
                    .await
            }
            StepProgress::Retry { message } => {
                // Retry this step.
                self.attempt += 1;
                let attempt_elapsed = now - self.attempt_start;
                self.attempt_start = Instant::now();

                // Send the retry message.
                self.sender
                    .send(Event::Step(StepEvent {
                        spec: S::schema_name(),
                        execution_id: self.execution_id,
                        event_index: (self.next_event_index)(),
                        total_elapsed: now - self.total_start,
                        kind: StepEventKind::AttemptRetry {
                            step: self.step_info.clone(),
                            next_attempt: self.attempt,
                            step_elapsed: now - self.step_start,
                            attempt_elapsed,
                            message,
                        },
                    }))
                    .await
            }
        }
    }

    async fn handle_nested(
        &mut self,
        now: Instant,
        event: Event<NestedSpec>,
    ) -> Result<(), ExecutionError<S>> {
        match event {
            Event::Step(event) => {
                self.sender
                    .send(Event::Step(StepEvent {
                        spec: S::schema_name(),
                        execution_id: self.execution_id,
                        event_index: (self.next_event_index)(),
                        total_elapsed: now - self.total_start,
                        kind: StepEventKind::Nested {
                            step: self.step_info.clone(),
                            attempt: self.attempt,
                            event: Box::new(event),
                            step_elapsed: now - self.step_start,
                            attempt_elapsed: now - self.attempt_start,
                        },
                    }))
                    .await
            }
            Event::Progress(event) => {
                self.sender
                    .send(Event::Progress(ProgressEvent {
                        spec: S::schema_name(),
                        execution_id: self.execution_id,
                        total_elapsed: now - self.total_start,
                        kind: ProgressEventKind::Nested {
                            step: self.step_info.clone(),
                            attempt: self.attempt,
                            event: Box::new(event),
                            step_elapsed: now - self.step_start,
                            attempt_elapsed: now - self.attempt_start,
                        },
                    }))
                    .await
            }
        }
    }

    async fn handle_abort(mut self, message: String) -> ExecutionError<S> {
        // Send the abort message over the channel.
        //
        // The only way this can fail is if the event receiver is closed or
        // dropped. That failure doesn't have any implications on whether this
        // aborts or not.
        let res = self
            .sender
            .send(Event::Step(StepEvent {
                spec: S::schema_name(),
                execution_id: self.execution_id,
                event_index: (self.next_event_index)(),
                total_elapsed: self.total_start.elapsed(),
                kind: StepEventKind::ExecutionAborted {
                    aborted_step: self.step_info.clone(),
                    attempt: self.attempt,
                    step_elapsed: self.step_start.elapsed(),
                    attempt_elapsed: self.attempt_start.elapsed(),
                    message: message.clone(),
                },
            }))
            .await;

        match res {
            Ok(()) => ExecutionError::Aborted {
                component: self.step_info.info.component.clone(),
                id: self.step_info.info.id.clone(),
                description: self.step_info.info.description.clone(),
                message,
            },
            Err(error) => error,
        }
    }

    async fn next_step(
        mut self,
        step_res: Result<StepOutcome<S>, S::Error>,
        next_step_info: &StepInfoWithMetadata<S>,
    ) -> Result<(), ExecutionError<S>> {
        match step_res {
            Ok(outcome) => {
                self.sender
                    .send(Event::Step(StepEvent {
                        spec: S::schema_name(),
                        execution_id: self.execution_id,
                        event_index: (self.next_event_index)(),
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
                let description = self.step_info.info.description.clone();
                self.send_error(&error).await?;
                Err(ExecutionError::StepFailed {
                    component,
                    id,
                    description,
                    error,
                })
            }
        }
    }

    async fn last_step(
        mut self,
        step_res: Result<StepOutcome<S>, S::Error>,
    ) -> Result<(), ExecutionError<S>> {
        match step_res {
            Ok(outcome) => {
                self.sender
                    .send(Event::Step(StepEvent {
                        spec: S::schema_name(),
                        execution_id: self.execution_id,
                        event_index: (self.next_event_index)(),
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
                let description = self.step_info.info.description.clone();
                self.send_error(&error).await?;
                Err(ExecutionError::StepFailed {
                    component,
                    id,
                    description,
                    error,
                })
            }
        }
    }

    async fn send_error(
        mut self,
        error: &S::Error,
    ) -> Result<(), ExecutionError<S>> {
        // Stringify `error` into a message + list causes; this is written the
        // way it is to avoid `error` potentially living across the `.await`
        // below (which can cause lifetime issues in callers).
        let (message, causes) = {
            let error = error.as_error();
            let message = error.to_string();

            let mut current = error;
            let mut causes = vec![];
            while let Some(source) = current.source() {
                causes.push(source.to_string());
                current = source;
            }
            (message, causes)
        };

        self.sender
            .send(Event::Step(StepEvent {
                spec: S::schema_name(),
                execution_id: self.execution_id,
                event_index: (self.next_event_index)(),
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

        let (engine, receiver): (UpdateEngine<TestSpec>, _) =
            UpdateEngine::new(&logctx.log);

        engine
            .new_step("foo".to_owned(), 0, "Step 1", |_| async {
                step_1_run = true;
                StepSuccess::new(()).into()
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
                StepSuccess::new(()).into()
            })
            .register();

        engine
            .execute()
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

        logctx.cleanup_successful();
    }
}
