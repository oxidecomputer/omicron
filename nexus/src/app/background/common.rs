// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # Nexus Background Tasks
//!
//! A **background task** in Nexus is any operation that can be activated both
//! periodically and by an explicit signal.  This is aimed at RFD 373-style
//! "reliable persistent workflows", also called "reconcilers" or "controllers".
//! These are a kind of automation that examines some _current_ state, compares
//! it to some _intended_ state, and potentially takes action to try to bring
//! the current state in sync with the intended state.  Our canonical example is
//! that we want to have Nexus monitor the intended DNS configuration.  When it
//! changes, we want to propagate the new configuration to all DNS servers.  We
//! implement this with three different background tasks:
//!
//! 1. `DnsConfigWatcher` reads the DNS configuration from the database, stores
//!    it in memory, and makes it available via a `tokio::sync::watch` channel.
//! 2. `DnsServersWatcher` reads the list of DNS servers from the database,
//!    stores it in memory, and makes it available via a `tokio::sync::watch`
//!    channel.
//! 3. `DnsPropagator` uses the the watch channels provided by the other two
//!    background tasks to notice when either the DNS configuration or the list
//!    of DNS servers has changed.  It uses the latest values to make a request
//!    to each server to update its configuration.
//!
//! When Nexus changes the DNS configuration, it will update the database with
//! the new configuration and then explicitly activate the `DnsConfigWatcher`.
//! When it reads the new config, it will send it to its watch channel, and that
//! will activate the `DnsPropagator`.  If any of this fails, or if Nexus
//! crashes at any point, then the periodic activation of every background task
//! will eventually cause the latest config to be propagated to all of the
//! current servers.
//!
//! The background task framework here is pretty minimal: essentially what it
//! gives you is that you just write an idempotent function that you want to
//! happen periodically or on-demand, wrap it in an impl of `BackgroundTask`,
//! register that with the `Driver`, and you're done.  The framework will take
//! care of:
//!
//! * providing a way for Nexus at-large to activate your task
//! * activating your task periodically
//! * ensuring that the task is activated only once at a time in this Nexus
//!   (but note that it may always be running concurrently in other Nexus
//!   instances)
//! * providing basic visibility into whether the task is running, when the task
//!   last ran, etc.
//!
//! We may well want to extend the framework as we build more tasks in general
//! and reconcilers specifically.  But we should be mindful not to create
//! footguns for ourselves!  See "Design notes" below.
//!
//! ## Notes for background task implementors
//!
//! Background tasks are not necessarily just for reconcilers.  That's just the
//! design center.  The first two DNS background tasks above aren't reconcilers
//! in any non-trivial sense.
//!
//! Background task activations do not accept input, by design.  See "Design
//! notes" below.
//!
//! Generally, you probably don't want to have your background task do retries.
//! If things fail, you rely on the periodic reactivation to try again.
//!
//! ## Design notes
//!
//! The underlying design for RFD 373-style reconcilers is inspired by a few
//! related principles:
//!
//! * the principle in distributed systems of having exactly one code path to
//!   achieve a thing, and then always using that path to do that thing (as
//!   opposed to having separate paths for, say, the happy path vs. failover,
//!   and having one of those paths rarely used)
//! * the [constant-work pattern][1], which basically suggests that a system can
//!   be more robust and scalable if it's constructed in a way that always does
//!   the same amount of work.  Imagine if we made requests to the DNS servers
//!   to incrementally update their config every time the DNS data changed.
//!   This system does more work as users make more requests.  During overloads,
//!   things can fall over.  Compare with a system whose frontend merely updates
//!   the DNS configuration that _should_ exist and whose backend periodically
//!   scans the complete intended state and then sets its own state accordingly.
//!   The backend does the same amount of work no matter how many requests were
//!   made, making it more resistant to overload.  A big downside of this
//!   approach is increased latency from the user making a request to seeing it
//!   applied.  This can be mitigated (sacrificing some, but not all, of the
//!   "constant work" property) by triggering a backend scan operation when user
//!   requests complete.
//! * the design pattern in distributed systems of keeping two copies of data in
//!   sync using both event notifications (like a changelog) _and_ periodic full
//!   scans.  The hope is that a full scan never finds a change that wasn't
//!   correctly sync'd, but incorporating an occasional full scan into the
//!   design ensures that such bugs are found and their impact repaired
//!   automatically.
//!
//! [1]: https://aws.amazon.com/builders-library/reliability-and-constant-work/
//!
//! Combining these, we get a design pattern for a "reconciler" where:
//!
//! * The reconciler is activated by explicit request (when we know it has work
//!   to do) _and_ periodically (to deal with all manner of transient failures)
//! * The reconciler's activity is idempotent: given the same underlying state
//!   (e.g., database state), it always attempts to do the same thing.
//! * Each activation of the reconciler accepts no input.  That is, even when we
//!   think we know what changed, we do not use that information.  This ensures
//!   that the reconciler really is idempotent and its actions are based solely
//!   on the state that it's watching.  Put differently: having reconcilers
//!   accept an explicit hint about what changed (and then doing something
//!   differently based on that) bifurcates the code: there's the common case
//!   where that hint is available and the rarely-exercised case when it's not
//!   (e.g., because Nexus crashed and it's the subsequent periodic activation
//!   that's propagating this change).  This is what we're trying to avoid.
//! * We do allow reconcilers to be triggered by a `tokio::sync::watch` channel
//!   -- but again, not using the _data_ from that channel.  There are two big
//!   advantages here: (1) reduced latency from when a change is made to when
//!   the reconciler applies it, and (2) (arguably another way to say the same
//!   thing) we can space out the periodic activations much further, knowing
//!   that most of the time we're not increasing latency by doing this.  This
//!   compromises the "constant-work" pattern a bit: we might wind up running
//!   the reconciler more often during busy times than during idle times, and we
//!   could find that overloads something.  However, the _operation_ of the
//!   reconciler can still be constant work, and there's no more than that
//!   amount of work going on at any given time.
//!
//!   `watch` channels are a convenient primitive here because they only store
//!   one value.  With a little care, we can ensure that the writer never blocks
//!   and the readers can all see the latest value.  (By design, reconcilers
//!   generally only care about the latest state of something, not any
//!   intermediate states.)  We don't have to worry about an unbounded queue, or
//!   handling a full queue, or other forms of backpressure.

use assert_matches::assert_matches;
use chrono::Utc;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::FutureExt;
use futures::StreamExt;
use nexus_db_queries::context::OpContext;
use nexus_types::internal_api::views::ActivationReason;
use nexus_types::internal_api::views::CurrentStatus;
use nexus_types::internal_api::views::CurrentStatusRunning;
use nexus_types::internal_api::views::LastResult;
use nexus_types::internal_api::views::LastResultCompleted;
use nexus_types::internal_api::views::TaskStatus;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::watch;
use tokio::sync::Notify;
use tokio::time::MissedTickBehavior;

/// An operation activated both periodically and by an explicit signal
///
/// See module-level documentation for details.
pub trait BackgroundTask: Send + Sync {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value>;
}

/// Drives the execution of background tasks
///
/// Nexus has only one Driver.  All background tasks are registered with the
/// Driver when Nexus starts up.  The Driver runs each background task in a
/// separate tokio task, primarily for runtime observability.  The Driver
/// provides interfaces for monitoring high-level state of each task (e.g., when
/// it last ran, whether it's currently running, etc.).
pub struct Driver {
    tasks: BTreeMap<TaskHandle, Task>,
}

/// Identifies a background task
///
/// This is returned by [`Driver::register()`] to identify the corresponding
/// background task.  It's then accepted by functions like
/// [`Driver::activate()`] and [`Driver::task_status()`] to identify the task.
#[derive(Clone, Debug, Ord, PartialOrd, PartialEq, Eq)]
pub struct TaskHandle(String);

impl TaskHandle {
    /// Returns the unique name of this background task
    pub fn name(&self) -> &str {
        &self.0
    }
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
    /// `Notify` used to wake up the tokio task when a caller explicit wants to
    /// activate the background task
    notify: Arc<Notify>,
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
    pub fn register(
        &mut self,
        name: String,
        description: String,
        period: Duration,
        imp: Box<dyn BackgroundTask>,
        opctx: OpContext,
        watchers: Vec<Box<dyn GenericWatcher>>,
    ) -> TaskHandle {
        // Activation of the background task happens in a separate tokio task.
        // Set up a channel so that tokio task can report status back to us.
        let (status_tx, status_rx) = watch::channel(TaskStatus {
            current: CurrentStatus::Idle,
            last: LastResult::NeverCompleted,
        });

        // Set up a channel so that we can wake up the tokio task if somebody
        // requests an explicit activation.
        let notify = Arc::new(Notify::new());

        // Spawn the tokio task that will manage activation of the background
        // task.
        let opctx = opctx.child(BTreeMap::from([(
            "background_task".to_string(),
            name.clone(),
        )]));
        let task_exec =
            TaskExec::new(period, imp, Arc::clone(&notify), opctx, status_tx);
        let tokio_task = tokio::task::spawn(task_exec.run(watchers));

        // Create an object to track our side of the background task's state.
        // This just provides the handles we need to read status and wake up the
        // tokio task.
        let task =
            Task { description, period, status: status_rx, tokio_task, notify };
        if self.tasks.insert(TaskHandle(name.clone()), task).is_some() {
            panic!("started two background tasks called {:?}", name);
        }

        // Return a handle that the caller can use to activate the task or get
        // its status.
        TaskHandle(name)
    }

    /// Enumerate all registered background tasks
    ///
    /// This is aimed at callers that want to get the status of all background
    /// tasks.  You'd call [`Driver::task_status()`] with each of the items
    /// produced by the iterator.
    pub fn tasks(&self) -> impl Iterator<Item = &TaskHandle> {
        self.tasks.keys()
    }

    fn task_required(&self, task: &TaskHandle) -> &Task {
        // It should be hard to hit this in practice, since you'd have to have
        // gotten a TaskHandle from somewhere.  It would have to be another
        // Driver instance.  But it's generally a singleton.
        self.tasks.get(task).unwrap_or_else(|| {
            panic!("attempted to get non-existent background task: {:?}", task)
        })
    }

    /// Returns a summary of what this task does (for developers)
    pub fn task_description(&self, task: &TaskHandle) -> &str {
        &self.task_required(task).description
    }

    /// Returns the configured period of the task
    pub fn task_period(&self, task: &TaskHandle) -> Duration {
        self.task_required(task).period
    }

    /// Activate the specified background task
    ///
    /// If the task is currently running, it will be activated again when it
    /// finishes.
    pub fn activate(&self, task: &TaskHandle) {
        self.task_required(task).notify.notify_one();
    }

    /// Returns the runtime status of the background task
    pub fn task_status(&self, task: &TaskHandle) -> TaskStatus {
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

/// Encapsulates state needed by the background tokio task to manage activation
/// of the background task
struct TaskExec {
    /// how often the background task should be activated
    period: Duration,
    /// impl of the background task
    imp: Box<dyn BackgroundTask>,
    /// used to receive notifications from the Driver that someone has requested
    /// explicit activation
    notify: Arc<Notify>,
    /// passed through to the background task impl when activated
    opctx: OpContext,
    /// used to send current status back to the Driver
    status_tx: watch::Sender<TaskStatus>,
    /// counts iterations of the task, for debuggability
    iteration: u64,
}

impl TaskExec {
    fn new(
        period: Duration,
        imp: Box<dyn BackgroundTask>,
        notify: Arc<Notify>,
        opctx: OpContext,
        status_tx: watch::Sender<TaskStatus>,
    ) -> TaskExec {
        TaskExec { period, imp, notify, opctx, status_tx, iteration: 0 }
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

                _ = self.notify.notified() => {
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
        let details = self.imp.activate(&self.opctx).await;

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
    use crate::app::background::common::ActivationReason;
    use crate::app::sagas::SagaRequest;
    use assert_matches::assert_matches;
    use chrono::Utc;
    use futures::future::BoxFuture;
    use futures::FutureExt;
    use nexus_db_queries::context::OpContext;
    use nexus_test_utils_macros::nexus_test;
    use std::time::Duration;
    use std::time::Instant;
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::error::TryRecvError;
    use tokio::sync::mpsc::Sender;
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
        let nexus = &cptestctx.server.apictx().nexus;
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
        let mut driver = Driver::new();

        assert_eq!(*rx1.borrow(), 0);
        let h1 = driver.register(
            "t1".to_string(),
            "test task".to_string(),
            Duration::from_millis(100),
            Box::new(t1),
            opctx.child(std::collections::BTreeMap::new()),
            vec![Box::new(dep_rx1.clone()), Box::new(dep_rx2.clone())],
        );

        let h2 = driver.register(
            "t2".to_string(),
            "test task".to_string(),
            Duration::from_secs(300), // should never fire in this test
            Box::new(t2),
            opctx.child(std::collections::BTreeMap::new()),
            vec![Box::new(dep_rx1.clone())],
        );

        let h3 = driver.register(
            "t3".to_string(),
            "test task".to_string(),
            Duration::from_secs(300), // should never fire in this test
            Box::new(t3),
            opctx,
            vec![Box::new(dep_rx1), Box::new(dep_rx2)],
        );

        // Wait for four activations of our task.  (This is three periods.) That
        // should take between 300ms and 400ms.  Allow extra time for a busy
        // system.
        let start = Instant::now();
        let wall_start = Utc::now();
        wait_until_count(rx1.clone(), 4).await;
        assert!(*rx1.borrow() == 4 || *rx1.borrow() == 5);
        let duration = start.elapsed();
        println!("rx1 -> 3 took {:?}", duration);
        assert!(
            duration.as_millis() < 1000,
            "took longer than 1s to activate our every-100ms-task three times"
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
        let nexus = &cptestctx.server.apictx().nexus;
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
        let h1 = driver.register(
            "t1".to_string(),
            "test task".to_string(),
            Duration::from_secs(300), // should not elapse during test
            Box::new(t1),
            opctx.child(std::collections::BTreeMap::new()),
            vec![Box::new(dep_rx1.clone())],
        );

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

    /// Simple BackgroundTask impl that sends a test-only SagaRequest
    struct SagaRequestTask {
        saga_request: Sender<SagaRequest>,
    }

    impl SagaRequestTask {
        fn new(saga_request: Sender<SagaRequest>) -> SagaRequestTask {
            SagaRequestTask { saga_request }
        }
    }

    impl BackgroundTask for SagaRequestTask {
        fn activate<'a>(
            &'a mut self,
            _: &'a OpContext,
        ) -> BoxFuture<'a, serde_json::Value> {
            async {
                let _ = self.saga_request.send(SagaRequest::TestOnly).await;
                serde_json::Value::Null
            }
            .boxed()
        }
    }

    #[nexus_test(server = crate::Server)]
    async fn test_saga_request_flow(cptestctx: &ControlPlaneTestContext) {
        let nexus = &cptestctx.server.apictx().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        let (saga_request, mut saga_request_recv) = SagaRequest::channel();
        let t1 = SagaRequestTask::new(saga_request);

        let mut driver = Driver::new();
        let (_dep_tx1, dep_rx1) = watch::channel(0);

        let h1 = driver.register(
            "t1".to_string(),
            "test saga request flow task".to_string(),
            Duration::from_secs(300), // should not fire in this test
            Box::new(t1),
            opctx.child(std::collections::BTreeMap::new()),
            vec![Box::new(dep_rx1.clone())],
        );

        assert!(matches!(
            saga_request_recv.try_recv(),
            Err(mpsc::error::TryRecvError::Empty),
        ));

        driver.activate(&h1);

        // wait 1 second for the saga request to arrive
        tokio::select! {
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(1)) => {
                assert!(false);
            }

            saga_request = saga_request_recv.recv() => {
                match saga_request {
                    None => {
                        assert!(false);
                    }

                    Some(saga_request) => {
                        assert!(matches!(
                            saga_request,
                            SagaRequest::TestOnly,
                        ));
                    }
                }
            }
        }
    }
}
