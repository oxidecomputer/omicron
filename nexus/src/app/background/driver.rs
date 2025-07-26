// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Generic system for managing execution of background tasks
//!
//! None of this file is specific to Nexus or any of the specific background
//! tasks in Nexus, although the design is pretty bespoke for what Nexus needs.

use crate::app::background::probes;

use super::BackgroundTask;
use super::TaskName;
use assert_matches::assert_matches;
use chrono::Utc;
use futures::FutureExt;
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use nexus_background_task_interface::Activator;
use nexus_db_queries::context::OpContext;
use nexus_types::internal_api::views::ActivationReason;
use nexus_types::internal_api::views::CurrentStatus;
use nexus_types::internal_api::views::CurrentStatusRunning;
use nexus_types::internal_api::views::LastResult;
use nexus_types::internal_api::views::LastResultCompleted;
use nexus_types::internal_api::views::TaskStatus;
use std::collections::BTreeMap;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::watch;
use tokio::time::MissedTickBehavior;

/// Drives the execution of background tasks
///
/// Nexus has only one Driver.  All background tasks are registered with the
/// Driver when Nexus starts up.  The Driver runs each background task in a
/// separate tokio task, primarily for runtime observability.  The Driver
/// provides interfaces for monitoring high-level state of each task (e.g., when
/// it last ran, whether it's currently running, etc.).
pub struct Driver {
    tasks: BTreeMap<TaskName, Task>,
}

/// Driver-side state of a background task
struct Task {
    /// what this task does (for developers)
    description: String,
    /// configured period of the task
    period: Duration,
    /// channel used to receive updates from the background task's tokio task
    /// about what the background task is doing
    status: watch::Receiver<TaskStatus>,
    /// join handle for the tokio task that's executing this background task
    tokio_task: tokio::task::JoinHandle<()>,
    /// `Activator` used to wake up the tokio task when a caller explicit wants to
    /// activate the background task
    activator: Activator,
}

impl Driver {
    pub fn new() -> Driver {
        Driver { tasks: BTreeMap::new() }
    }

    /// Register a new background task
    ///
    /// The task will be available to activate immediately by any callers using
    /// [`Driver::activate()`].  The Driver activates background tasks that have
    /// not run for duration `period`.
    ///
    /// `imp` is an impl of [`BackgroundTask`] that represents the work of the
    /// task.  This defines a function that gets invoked when the task is
    /// _activated_.  The activation function accepts `opctx`, an [`OpContext`]
    /// to be used for any actions taken by the background task.
    ///
    /// All background tasks have a unique `name` for observability.  This
    /// function panics if the name conflicts with that of a
    /// previously-registered task.
    ///
    /// `watchers` is a (possibly-empty) list of
    /// [`tokio::sync::watch::Receiver`] objects.  The Driver will automatically
    /// activate the background task when any of these receivers' data has
    /// changed.  This can be used to create dependencies between background
    /// tasks, so that when one of them finishes doing something, it kicks off
    /// another one.
    ///
    /// `activator` is an [`Activator`] that has not previously been used in a
    /// call to this function.  It will be wired up so that using it will
    /// activate this newly-registered background task.  This is an argument
    /// rather than a returned value because it's useful for consumers to create
    /// these ahead of time, before tasks have been set up.  See [`super::init`]
    /// module-level docs for more on this.
    ///
    /// # Panics
    ///
    /// This function panics if the `name` or `activator` has previously been
    /// passed to a call to this function.
    pub fn register<N, D>(
        &mut self,
        taskdef: TaskDefinition<'_, N, D>,
    ) -> TaskName
    where
        N: ToString,
        D: ToString,
    {
        let name = taskdef.name.to_string();

        // Activation of the background task happens in a separate tokio task.
        // Set up a channel so that tokio task can report status back to us.
        let (status_tx, status_rx) = watch::channel(TaskStatus {
            current: CurrentStatus::Idle,
            last: LastResult::NeverCompleted,
        });

        // We'll use a `Notify` to wake up that tokio task when an activation is
        // requested.  The caller provides their own Activator, which just
        // provides a specific Notify for us to use here.
        let activator = taskdef.activator;
        if let Err(error) = activator.mark_wired_up() {
            panic!(
                "{error}: currently attempting to wire it up to task {:?}",
                name
            );
        }

        // Spawn the tokio task that will manage activation of the background
        // task.
        let opctx = taskdef.opctx.child(BTreeMap::from([(
            "background_task".to_string(),
            name.clone(),
        )]));
        let task_exec = TaskExec::new(
            &name,
            taskdef.period,
            taskdef.task_impl,
            activator.clone(),
            opctx,
            status_tx,
        );
        let tokio_task = tokio::task::spawn(task_exec.run(taskdef.watchers));

        // Create an object to track our side of the background task's state.
        // This just provides the handles we need to read status and wake up the
        // tokio task.
        let task = Task {
            description: taskdef.description.to_string(),
            period: taskdef.period,
            status: status_rx,
            tokio_task,
            activator: activator.clone(),
        };
        if self.tasks.insert(TaskName(name.clone()), task).is_some() {
            panic!("started two background tasks called {:?}", name);
        }

        // Return a handle that the caller can use to activate the task or get
        // its status.
        TaskName(name)
    }

    /// Enumerate all registered background tasks
    ///
    /// This is aimed at callers that want to get the status of all background
    /// tasks.  You'd call [`Driver::task_status()`] with each of the items
    /// produced by the iterator.
    pub fn tasks(&self) -> impl Iterator<Item = &TaskName> {
        self.tasks.keys()
    }

    fn task_required(&self, task: &TaskName) -> &Task {
        // It should be hard to hit this in practice, since you'd have to have
        // gotten a TaskHandle from somewhere.  It would have to be another
        // Driver instance.  But it's generally a singleton.
        self.tasks.get(task).unwrap_or_else(|| {
            panic!("attempted to get non-existent background task: {:?}", task)
        })
    }

    /// Returns a summary of what this task does (for developers)
    pub fn task_description(&self, task: &TaskName) -> &str {
        &self.task_required(task).description
    }

    /// Returns the configured period of the task
    pub fn task_period(&self, task: &TaskName) -> Duration {
        self.task_required(task).period
    }

    /// Activate the specified background task
    ///
    /// If the task is currently running, it will be activated again when it
    /// finishes.
    pub(super) fn activate(&self, task: &TaskName) {
        probes::background__task__activate!(|| task.as_str());
        self.task_required(task).activator.activate();
    }

    /// Returns the runtime status of the background task
    pub fn task_status(&self, task: &TaskName) -> TaskStatus {
        // Borrowing from a watch channel's receiver blocks the sender.  Clone
        // the status to avoid an errant caller gumming up the works by hanging
        // on to a reference.
        self.task_required(task).status.borrow().clone()
    }
}

impl Drop for Driver {
    fn drop(&mut self) {
        // When the driver is dropped, terminate all tokio tasks that were used
        // to run background tasks.
        for (_, t) in &self.tasks {
            t.tokio_task.abort();
        }
    }
}

/// Describes a background task to be registered with [`Driver::register()`]
///
/// See [`Driver::register()`] for more on how these fields get used.
pub struct TaskDefinition<'a, N: ToString, D: ToString> {
    /// identifier for this task
    pub name: N,
    /// short human-readable summary of this task
    pub description: D,
    /// driver should activate the task if it hasn't run in this long
    pub period: Duration,
    /// impl of [`BackgroundTask`] that represents the work of the task
    pub task_impl: Box<dyn BackgroundTask>,
    /// `OpContext` used for task activations
    pub opctx: OpContext,
    /// list of watchers that will trigger activation of this task
    pub watchers: Vec<Box<dyn GenericWatcher>>,
    /// an [`Activator]` that will be wired up to activate this task
    pub activator: &'a Activator,
}

/// Encapsulates state needed by the background tokio task to manage activation
/// of the background task
struct TaskExec {
    /// the name of this background task
    name: String,
    /// how often the background task should be activated
    period: Duration,
    /// impl of the background task
    imp: Box<dyn BackgroundTask>,
    /// used to receive notifications from the Driver that someone has requested
    /// explicit activation
    activation: Activator,
    /// passed through to the background task impl when activated
    opctx: OpContext,
    /// used to send current status back to the Driver
    status_tx: watch::Sender<TaskStatus>,
    /// counts iterations of the task, for debuggability
    iteration: u64,
}

impl TaskExec {
    fn new(
        name: impl ToString,
        period: Duration,
        imp: Box<dyn BackgroundTask>,
        activation: Activator,
        opctx: OpContext,
        status_tx: watch::Sender<TaskStatus>,
    ) -> TaskExec {
        let name = name.to_string();
        TaskExec {
            name,
            period,
            imp,
            activation,
            opctx,
            status_tx,
            iteration: 0,
        }
    }

    /// Body of the tokio task that manages activation of this background task
    async fn run(mut self, mut deps: Vec<Box<dyn GenericWatcher>>) {
        let mut interval = tokio::time::interval(self.period);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        // Wait for either the timeout to elapse, or an explicit activation
        // signal from the Driver, or for one of our dependencies ("watch"
        // channels) to trigger an activation.
        loop {
            let mut dependencies: FuturesUnordered<_> =
                deps.iter_mut().map(|w| w.wait_for_change()).collect();

            tokio::select! {
                _ = interval.tick() => {
                    self.activate(ActivationReason::Timeout).await;
                },

                _ = self.activation.activated() => {
                    self.activate(ActivationReason::Signaled).await;
                }

                _ = dependencies.next(), if !dependencies.is_empty() => {
                    self.activate(ActivationReason::Dependency).await;
                }
            }
        }
    }

    /// "Activate" the background task
    ///
    /// This basically just invokes `activate()` on the underlying
    /// `BackgroundTask` impl, but provides a bunch of runtime observability
    /// around doing so.
    async fn activate(&mut self, reason: ActivationReason) {
        self.iteration += 1;
        let iteration = self.iteration;
        let start_time = Utc::now();
        let start_instant = Instant::now();

        debug!(
            &self.opctx.log,
            "activating";
            "reason" => ?reason,
            "iteration" => iteration
        );

        // Update our status with the driver.
        self.status_tx.send_modify(|status| {
            assert_matches!(status.current, CurrentStatus::Idle);
            status.current = CurrentStatus::Running(CurrentStatusRunning {
                start_time,
                start_instant,
                reason,
                iteration,
            });
        });

        // Do it!
        probes::background__task__activate__start!(|| {
            (&self.name, self.iteration, format!("{reason:?}"))
        });
        let details = self.imp.activate(&self.opctx).await;
        let details_str = serde_json::to_string(&details).unwrap_or_else(|e| {
            format!("<<failed to serialize task status: {}>>", e)
        });
        probes::background__task__activate__done!(|| {
            (&self.name, self.iteration, &details_str)
        });

        let elapsed = start_instant.elapsed();

        // Update our status with the driver.
        self.status_tx.send_modify(|status| {
            assert!(!status.current.is_idle());
            let current = status.current.unwrap_running();
            assert_eq!(current.iteration, iteration);
            *status = TaskStatus {
                current: CurrentStatus::Idle,
                last: LastResult::Completed(LastResultCompleted {
                    iteration,
                    start_time,
                    reason,
                    elapsed,
                    details,
                }),
            };
        });

        debug!(
            &self.opctx.log,
            "activation complete";
            "elapsed" => ?elapsed,
            "iteration" => iteration,
            "status" => details_str,
        );
    }
}

/// Used to erase the specific type of a `tokio::sync::watch::Receiver`
///
/// This allows the `Driver` to treat these generically, activating a task when
/// any of the watch channels changes, regardless of what data is stored in the
/// channel.
pub trait GenericWatcher: Send {
    fn wait_for_change(
        &mut self,
    ) -> BoxFuture<'_, Result<(), watch::error::RecvError>>;
}

impl<T: Send + Sync> GenericWatcher for watch::Receiver<T> {
    fn wait_for_change(
        &mut self,
    ) -> BoxFuture<'_, Result<(), watch::error::RecvError>> {
        async { self.changed().await }.boxed()
    }
}

#[cfg(test)]
mod test {
    use super::BackgroundTask;
    use super::Driver;
    use crate::app::background::Activator;
    use crate::app::background::driver::TaskDefinition;
    use assert_matches::assert_matches;
    use chrono::Utc;
    use futures::FutureExt;
    use futures::future::BoxFuture;
    use nexus_db_queries::context::OpContext;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::internal_api::views::ActivationReason;
    use std::time::Duration;
    use std::time::Instant;
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::error::TryRecvError;
    use tokio::sync::watch;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    /// Simple BackgroundTask impl that just reports how many times it's run.
    struct ReportingTask {
        counter: usize,
        tx: watch::Sender<usize>,
    }

    impl ReportingTask {
        fn new() -> (ReportingTask, watch::Receiver<usize>) {
            let (tx, rx) = watch::channel(0);
            (ReportingTask { counter: 1, tx }, rx)
        }
    }

    impl BackgroundTask for ReportingTask {
        fn activate<'a>(
            &'a mut self,
            _: &'a OpContext,
        ) -> BoxFuture<'a, serde_json::Value> {
            async {
                let count = self.counter;
                self.counter += 1;
                self.tx.send_replace(count);
                serde_json::Value::Number(serde_json::Number::from(count))
            }
            .boxed()
        }
    }

    async fn wait_until_count(mut rx: watch::Receiver<usize>, count: usize) {
        loop {
            let v = rx.borrow_and_update();
            assert!(*v <= count, "count went past what we expected");
            if *v == count {
                return;
            }
            drop(v);

            tokio::time::timeout(Duration::from_secs(5), rx.changed())
                .await
                .unwrap()
                .unwrap();
        }
    }

    // Verifies that activation through each of the three mechanisms (explicit
    // signal, timeout, or dependency) causes exactly the right tasks to be
    // activated
    #[nexus_test(server = crate::Server)]
    async fn test_driver_basic(cptestctx: &ControlPlaneTestContext) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        // Create up front:
        //
        // - three ReportingTasks (our background tasks)
        // - two "watch" channels used as dependencies for these tasks

        let (t1, rx1) = ReportingTask::new();
        let (t2, rx2) = ReportingTask::new();
        let (t3, rx3) = ReportingTask::new();
        let (dep_tx1, dep_rx1) = watch::channel(0);
        let (dep_tx2, dep_rx2) = watch::channel(0);
        let act1 = Activator::new();
        let act2 = Activator::new();
        let act3 = Activator::new();
        let mut driver = Driver::new();

        assert_eq!(*rx1.borrow(), 0);
        let h1 = driver.register(TaskDefinition {
            name: "t1",
            description: "test task",
            period: Duration::from_millis(100),
            task_impl: Box::new(t1),
            opctx: opctx.child(std::collections::BTreeMap::new()),
            watchers: vec![
                Box::new(dep_rx1.clone()),
                Box::new(dep_rx2.clone()),
            ],
            activator: &act1,
        });

        let h2 = driver.register(TaskDefinition {
            name: "t2",
            description: "test task",
            period: Duration::from_secs(300), // should never fire in this test
            task_impl: Box::new(t2),
            opctx: opctx.child(std::collections::BTreeMap::new()),
            watchers: vec![Box::new(dep_rx1.clone())],
            activator: &act2,
        });

        let h3 = driver.register(TaskDefinition {
            name: "t3",
            description: "test task",
            period: Duration::from_secs(300), // should never fire in this test
            task_impl: Box::new(t3),
            opctx,
            watchers: vec![Box::new(dep_rx1), Box::new(dep_rx2)],
            activator: &act3,
        });

        // Wait for four activations of our task.  (This is three periods.) That
        // would usually take between 300ms and 400ms, but allow plenty of time
        // as a buffer.
        let start = Instant::now();
        let wall_start = Utc::now();
        wait_until_count(rx1.clone(), 4).await;
        assert!(*rx1.borrow() == 4 || *rx1.borrow() == 5);
        let duration = start.elapsed();
        println!("rx1 -> 3 took {:?}", duration);
        assert!(
            duration < Duration::from_secs(30),
            "took longer than 30s to activate our \
             every-100ms-task three times"
        );
        assert!(duration.as_millis() >= 300);
        // Check how the last activation was reported.
        let status = driver.task_status(&h1);
        let last = status.last.unwrap_completion();
        // It's conceivable that there's been another activation already.
        assert!(last.iteration == 3 || last.iteration == 4);
        assert!(last.start_time >= wall_start);
        assert!(last.start_time <= Utc::now());
        assert!(last.elapsed <= duration);
        assert_matches!(
            last.details,
            serde_json::Value::Number(n)
                if n.as_u64().unwrap() == last.iteration
        );

        // Tasks "t2" and "t3" ought to have seen only one activation in this
        // time, from its beginning-of-time activation.
        assert_eq!(*rx2.borrow(), 1);
        assert_eq!(*rx3.borrow(), 1);
        let status = driver.task_status(&h2);
        let last = status.last.unwrap_completion();
        assert_eq!(last.iteration, 1);
        let status = driver.task_status(&h3);
        let last = status.last.unwrap_completion();
        assert_eq!(last.iteration, 1);

        // Explicitly wake up all of our tasks by reporting that dep1 has
        // changed.
        println!("firing dependency tx1");
        dep_tx1.send_replace(1);
        wait_until_count(rx2.clone(), 2).await;
        wait_until_count(rx3.clone(), 2).await;
        assert_eq!(*rx2.borrow(), 2);
        assert_eq!(*rx3.borrow(), 2);
        let status = driver.task_status(&h2);
        let last = status.last.unwrap_completion();
        assert_eq!(last.iteration, 2);
        let status = driver.task_status(&h3);
        let last = status.last.unwrap_completion();
        assert_eq!(last.iteration, 2);

        // Explicitly wake up just "t3" by reporting that dep2 has changed.
        println!("firing dependency tx2");
        dep_tx2.send_replace(1);
        wait_until_count(rx3.clone(), 3).await;
        assert_eq!(*rx2.borrow(), 2);
        assert_eq!(*rx3.borrow(), 3);
        let status = driver.task_status(&h2);
        let last = status.last.unwrap_completion();
        assert_eq!(last.iteration, 2);
        let status = driver.task_status(&h3);
        let last = status.last.unwrap_completion();
        assert_eq!(last.iteration, 3);

        // Explicitly activate just "t3".
        driver.activate(&h3);
        wait_until_count(rx3.clone(), 4).await;
        assert_eq!(*rx2.borrow(), 2);
        assert_eq!(*rx3.borrow(), 4);
        let status = driver.task_status(&h2);
        let last = status.last.unwrap_completion();
        assert_eq!(last.iteration, 2);
        let status = driver.task_status(&h3);
        let last = status.last.unwrap_completion();
        assert_eq!(last.iteration, 4);

        // Explicitly activate just "t2", this time using its Activator.
        act2.activate();
        wait_until_count(rx2.clone(), 3).await;
        assert_eq!(*rx3.borrow(), 4);
        let status = driver.task_status(&h2);
        let last = status.last.unwrap_completion();
        assert_eq!(last.iteration, 3);
        let status = driver.task_status(&h3);
        let last = status.last.unwrap_completion();
        assert_eq!(last.iteration, 4);
    }

    /// Simple background task that moves in lockstep with a consumer, allowing
    /// the creator to be notified when it becomes active and to determine when
    /// the activation finishes.
    struct PausingTask {
        counter: usize,
        ready_tx: mpsc::Sender<usize>,
        wait_rx: mpsc::Receiver<()>,
    }

    impl PausingTask {
        fn new(
            wait_rx: mpsc::Receiver<()>,
        ) -> (PausingTask, mpsc::Receiver<usize>) {
            let (ready_tx, ready_rx) = mpsc::channel(10);
            (PausingTask { counter: 1, wait_rx, ready_tx }, ready_rx)
        }
    }

    impl BackgroundTask for PausingTask {
        fn activate<'a>(
            &'a mut self,
            _: &'a OpContext,
        ) -> BoxFuture<'a, serde_json::Value> {
            async {
                let count = self.counter;
                self.counter += 1;
                let _ = self.ready_tx.send(count).await;
                let _ = self.wait_rx.recv().await;
                serde_json::Value::Null
            }
            .boxed()
        }
    }

    // Exercises various case of activation while a background task is currently
    // activated.
    #[nexus_test(server = crate::Server)]
    async fn test_activation_in_progress(cptestctx: &ControlPlaneTestContext) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        let mut driver = Driver::new();
        let (tx1, rx1) = mpsc::channel(10);
        let (t1, mut ready_rx1) = PausingTask::new(rx1);
        let (dep_tx1, dep_rx1) = watch::channel(0);
        let before_wall = Utc::now();
        let before_instant = Instant::now();
        let act1 = Activator::new();
        let h1 = driver.register(TaskDefinition {
            name: "t1",
            description: "test task",
            period: Duration::from_secs(300), // should not elapse during test
            task_impl: Box::new(t1),
            opctx: opctx.child(std::collections::BTreeMap::new()),
            watchers: vec![Box::new(dep_rx1.clone())],
            activator: &act1,
        });

        // Wait to enter the first activation.
        let which = ready_rx1.recv().await.unwrap();
        assert_eq!(which, 1);
        let after_wall = Utc::now();
        let after_instant = Instant::now();
        // Verify that it's a timeout-based activation.
        let status = driver.task_status(&h1);
        assert!(!status.last.has_completed());
        let current = status.current.unwrap_running();
        assert!(current.start_time >= before_wall);
        assert!(current.start_time <= after_wall);
        assert!(current.start_instant >= before_instant);
        assert!(current.start_instant <= after_instant);
        assert_eq!(current.iteration, 1);
        assert_eq!(current.reason, ActivationReason::Timeout);
        // Enqueue another activation by dependency while this one is still
        // running.
        dep_tx1.send_replace(1);
        // Complete the activation.
        tx1.send(()).await.unwrap();

        // We should immediately see another activation.
        let which = ready_rx1.recv().await.unwrap();
        assert_eq!(which, 2);
        assert!(after_instant.elapsed().as_millis() < 5000);
        // Verify that it's a dependency-caused activation.
        let status = driver.task_status(&h1);
        let last = status.last.unwrap_completion();
        assert_eq!(last.start_time, current.start_time);
        assert_eq!(last.iteration, current.iteration);
        let current = status.current.unwrap_running();
        assert!(current.start_time >= after_wall);
        assert!(current.start_instant >= after_instant);
        assert_eq!(current.iteration, 2);
        assert_eq!(current.reason, ActivationReason::Dependency);
        // Enqueue another activation by explicit signal while this one is still
        // running.
        driver.activate(&h1);
        // Complete the activation.
        tx1.send(()).await.unwrap();

        // We should immediately see another activation.
        let which = ready_rx1.recv().await.unwrap();
        assert_eq!(which, 3);
        assert!(after_instant.elapsed().as_millis() < 10000);
        // Verify that it's a signal-caused activation.
        let status = driver.task_status(&h1);
        let last = status.last.unwrap_completion();
        assert_eq!(last.start_time, current.start_time);
        assert_eq!(last.iteration, current.iteration);
        let current = status.current.unwrap_running();
        assert_eq!(current.iteration, 3);
        assert_eq!(current.reason, ActivationReason::Signaled);
        // This time, queue up several explicit activations.
        driver.activate(&h1);
        driver.activate(&h1);
        driver.activate(&h1);
        tx1.send(()).await.unwrap();

        // Again, we should see an activation basically immediately.
        let which = ready_rx1.recv().await.unwrap();
        assert_eq!(which, 4);
        tx1.send(()).await.unwrap();

        // But we should not see any more activations.  Those multiple
        // notifications should have gotten collapsed.  It is hard to know
        // there's not another one coming, so we just wait long enough that we
        // expect to have seen it if it is coming.
        tokio::time::sleep(Duration::from_secs(1)).await;
        let status = driver.task_status(&h1);
        assert!(status.current.is_idle());
        assert_eq!(status.last.unwrap_completion().iteration, 4);
        assert_matches!(ready_rx1.try_recv(), Err(TryRecvError::Empty));

        // Now, trigger several dependency-based activations.  We should see the
        // same result: these get collapsed.
        dep_tx1.send_replace(2);
        dep_tx1.send_replace(3);
        dep_tx1.send_replace(4);
        let which = ready_rx1.recv().await.unwrap();
        assert_eq!(which, 5);
        tx1.send(()).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
        let status = driver.task_status(&h1);
        assert!(status.current.is_idle());
        assert_eq!(status.last.unwrap_completion().iteration, 5);
        assert_matches!(ready_rx1.try_recv(), Err(TryRecvError::Empty));

        // It would be nice to also verify that multiple time-based activations
        // also get collapsed, but this is a fair bit trickier.  Using the same
        // approach, we'd need to wait long enough that we'd catch any
        // _erroneous_ activation, but not so long that we might catch the next
        // legitimate periodic activation.  It's hard to choose a period for
        // such a task that would allow us to reliably distinguish between these
        // two without also spending a lot of wall-clock time on this test.
    }
}
