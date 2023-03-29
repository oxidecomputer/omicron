use std::borrow::Cow;

use debug_ignore::DebugIgnore;
use futures::{future::BoxFuture, prelude::*};
use tokio::sync::mpsc;

use crate::update_events::UpdateTerminalEventKind;

#[derive(Debug)]
pub(crate) struct UpdateExecutor<'a> {
    // TODO: for now, this is a sequential series of steps. This can potentially
    // be a graph in the future.
    log: slog::Logger,
    steps: Vec<UpdateStep<'a>>,
}

impl<'a> UpdateExecutor<'a> {
    pub(crate) fn new(log: &slog::Logger) -> Self {
        Self {
            log: log.new(slog::o!("component" => "UpdateExecutor")),
            steps: Vec::new(),
        }
    }

    /// It is currently expected that this is called exactly once per major
    /// component.
    pub(crate) fn new_component(
        &mut self,
        component: MajorComponent,
    ) -> StepRegistrar<'_, 'a> {
        StepRegistrar { executor: self, component }
    }
}

// Note: have to be careful with lifetimes here because 'a is an invariant
// lifetime. If there are compile errors related to this, they're likely to be
// because 'a got mixed up with a covariant lifetime like 'exec.
pub(crate) struct StepRegistrar<'exec, 'a> {
    executor: &'exec mut UpdateExecutor<'a>,
    component: MajorComponent,
}

impl<'exec, 'a> StepRegistrar<'exec, 'a> {
    pub(crate) fn register_step<F, Fut>(
        &mut self,
        description: impl Into<Cow<'static, str>>,
        step_fn: F,
    ) where
        F: FnOnce(UpdateExecContext) -> Fut + 'a,
        Fut: Future<Output = Result<(), UpdateTerminalEventKind>> + Send + 'a,
    {
        let exec = Box::new(move |cx: UpdateExecContext| (step_fn)(cx).boxed());
        let step = UpdateStep {
            component: self.component,
            description: description.into(),
            exec: Some(DebugIgnore(exec)),
        };
        self.executor.steps.push(step);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum MajorComponent {
    Rot,
    Sp,
    Host,
    ControlPlane,
}

#[derive(Clone, Debug)]
pub(crate) struct UpdateExecContext {
    pub(crate) sender: mpsc::Sender<StepProgress>,
}

#[derive(Debug)]
pub(crate) enum StepProgress {
    /// Running without progress.
    ///
    /// This is the starting state. Prefer the `Progress` variant if possible.
    Running,

    /// Running with progress.
    Progress { attempt: usize, current: u64, total: u64 },

    /// Completed.
    Finished,
}

#[derive(Debug)]
struct UpdateStep<'a> {
    component: MajorComponent,
    description: Cow<'static, str>,
    // This starts off as Some, then becomes None once this step is executed.
    exec: Option<DebugIgnore<StepExecFn<'a>>>,
}

type StepExecFn<'a> = Box<
    dyn FnOnce(
            UpdateExecContext,
        ) -> BoxFuture<'a, Result<(), UpdateTerminalEventKind>>
        + 'a,
>;
