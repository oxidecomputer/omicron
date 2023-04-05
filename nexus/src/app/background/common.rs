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
//! the current state in sync with the intended state.
//!
//! Our canonical example is that we want to have Nexus monitor the intended DNS
//! configuration.  When it changes, we want to propagate the new configuration
//! to all DNS servers.  We implement this with three different background
//! tasks:
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
//! When it reads the new config, that will activate the `DnsPropagator`.  If
//! any of this fails, or if Nexus crashes at any point, then the periodic
//! activation of every background task will eventually cause the latest config
//! to be propagated to all of the current servers.
//!
//! The "framework" here is pretty minimal: essentially what it gives you is
//! that for the most part you can just write an idempotent function that you
//! want to happen periodically or on-demand, wrap it in an impl of
//! `BackgroundTask`, register that with the `Driver`, and you're done.  The
//! framework will take care of:
//!
//! * ensuring that the function is only called once concurrently
//! * providing a way for Nexus at-large to activate your task
//! * activating your task periodically
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
//!   opposed to having separate paths for, say, the happy path vs. failover)
//! * the [constant-work pattern][1], which basically suggests that a system can
//!   be more robust and scalable if it's constructed in a way that always does
//!   the same amount of work.  Consider a system that emits events when the
//!   user takes some action and then processes these events individually to
//!   change its state.  This system does more work as the user makes more
//!   requests.  During overloads, things can fall over.  Compare with a system
//!   whose frontend merely updates some intended state when the user asks to
//!   change it, and whose backend periodically scans the complete intended
//!   state and then sets its own state accordingly.  The backend does the same
//!   amount of work no matter how many requests were made, making it more
//!   resistant to overload.  A big downside of this approach is increased
//!   latency from the user making a request to seeing it applied.  This can be
//!   mitigated (sacrificing some, but not all, of the "constant work" upside)
//!   by triggering a backend scan operation when user requests complete.
//! * the design pattern in distributed systems of keeping two copies of data in
//!   sync using both event notifications (like a changelog) _and_ periodic full
//!   scans.  The hope is that a full scan never finds a change that wasn't
//!   correctly sync'd, but incorporating it into the design ensures that such
//!   bugs are found and their impact repaired automatically.
//!
//! [1] https://aws.amazon.com/builders-library/reliability-and-constant-work/
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
//!   on the state that it watches.  Put differently: having reconcilers accept
//!   explicit hints or advice about what changed introduces the possibility
//!   that that state changes what it would do, and _that_ introduces the
//!   possibility that if it fails to do so for whatever reason, the subsequent
//!   periodic activation (which is supposed to fix things) won't work
//! * We do allow reconcilers to be triggered by a `tokio::sync::watch` channel.
//!   There are two big advantages here: (1) reduced latency from when
//!   something happens that requires the reconciler to do something to having
//!   it do it, and (2) (arguably another way to say the same thing) we can
//!   space out the periodic activations much further, knowing that most of the
//!   time we're not increasing latency by doing this.  This compromises the
//!   "constant-work" pattern a bit -- we might wind up running the reconciler
//!   more often during busy times than during idle times, and we could find
//!   that overloads something.  However, the _operation_ of the reconciler can
//!   still be constant work.
//!
//!   `watch` channels are a convenient primitive here because they only store
//!   one value.  With a little care, we can ensure that the writer never blocks
//!   and the readers can all see the latest value.  (By design, reconcilers
//!   generally only care about the latest state of something, not any
//!   intermediate states.)  We don't have to worry about an unbounded queue, or
//!   handling a full queue, or other forms of backpressure.

// TODO-doc more docs for the whole PR

use chrono::DateTime;
use chrono::Utc;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::FutureExt;
use futures::StreamExt;
use nexus_db_queries::context::OpContext;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::watch;
use tokio::sync::Notify;
use tokio::time::MissedTickBehavior;

/// An operation activated both periodically and by an explicit signal
///
/// See module-level documentation for details
pub trait BackgroundTask: Send + Sync {
    fn activate<'a, 'b, 'c>(
        &'a mut self,
        opctx: &'b OpContext,
    ) -> BoxFuture<'c, ()>
    where
        'a: 'c,
        'b: 'c;
}

struct Task {
    status: watch::Receiver<TaskStatus>,
    tokio_task: tokio::task::JoinHandle<()>,
    notify: Arc<Notify>,
}

pub struct Driver {
    tasks: BTreeMap<TaskHandle, Task>,
}

#[derive(Debug, Ord, PartialOrd, PartialEq, Eq)]
pub struct TaskHandle(String);

impl Driver {
    pub fn new() -> Driver {
        Driver { tasks: BTreeMap::new() }
    }

    pub fn register(
        &mut self,
        name: String,
        period: Duration,
        imp: Box<dyn BackgroundTask>,
        opctx: OpContext,
        watchers: Vec<Box<dyn GenericWatcher>>,
    ) -> TaskHandle {
        let (status_tx, status_rx) =
            watch::channel(TaskStatus { current: None, last: None });
        let notify = Arc::new(Notify::new());

        let opctx = opctx.child(BTreeMap::from([(
            "background_task".to_string(),
            name.clone(),
        )]));
        let task_exec =
            TaskExec::new(period, imp, Arc::clone(&notify), opctx, status_tx);
        let tokio_task = tokio::task::spawn(task_exec.run(watchers));

        let task = Task { status: status_rx, tokio_task, notify };
        if self.tasks.insert(TaskHandle(name.clone()), task).is_some() {
            panic!("started two background tasks called {:?}", name);
        }

        TaskHandle(name)
    }

    pub fn tasks(&self) -> impl Iterator<Item = &TaskHandle> {
        self.tasks.keys()
    }

    pub fn wakeup(&self, task: &TaskHandle) {
        // It should be hard to hit this in practice, since you'd have to have
        // gotten a TaskHandle from somewhere.  It would have to be another
        // Driver instance.
        let task = self.tasks.get(task).unwrap_or_else(|| {
            panic!(
                "attempted to wake up non-existent background task: {:?}",
                task
            )
        });

        task.notify.notify_one();
    }

    pub fn status(&self, task: &TaskHandle) -> TaskStatus {
        // It should be hard to hit this in practice, since you'd have to have
        // gotten a TaskHandle from somewhere.  It would have to be another
        // Driver instance.
        let task = self.tasks.get(task).unwrap_or_else(|| {
            panic!(
                "attempted to get status of non-existent background task: {:?}",
                task
            )
        });

        task.status.borrow().clone()
    }
}

impl Drop for Driver {
    fn drop(&mut self) {
        for (_, t) in &self.tasks {
            t.tokio_task.abort();
        }
    }
}

struct TaskExec {
    period: Duration,
    imp: Box<dyn BackgroundTask>,
    notify: Arc<Notify>,
    opctx: OpContext,
    status_tx: watch::Sender<TaskStatus>,
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

    async fn run(mut self, mut deps: Vec<Box<dyn GenericWatcher>>) {
        let mut interval = tokio::time::interval(self.period);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

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

                _ = dependencies.next(), if dependencies.len() > 0 => {
                    self.activate(ActivationReason::Dependency).await;
                }
            }
        }
    }

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

        self.status_tx.send_modify(|status| {
            assert!(status.current.is_none());
            status.current = Some(LastStart {
                start_time,
                start_instant,
                reason,
                iteration,
            });
        });

        self.imp.activate(&self.opctx).await;

        let elapsed = start_instant.elapsed();

        self.status_tx.send_modify(|status| {
            assert!(status.current.is_some());
            let current = status.current.as_ref().unwrap();
            assert_eq!(current.iteration, iteration);
            *status = TaskStatus {
                current: None,
                last: Some(LastResult {
                    iteration,
                    start_time: current.start_time,
                    elapsed,
                    value: serde_json::Value::Null,
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

#[derive(Debug, Clone, Copy)]
pub enum ActivationReason {
    Signaled,
    Timeout,
    Dependency,
}

#[derive(Clone, Debug)]
pub struct TaskStatus {
    pub current: Option<LastStart>,
    pub last: Option<LastResult>,
}

#[derive(Clone, Debug)]
pub struct LastStart {
    pub start_time: DateTime<Utc>,
    pub start_instant: Instant,
    pub reason: ActivationReason,
    pub iteration: u64,
}

#[derive(Clone, Debug)]
pub struct LastResult {
    pub iteration: u64,
    pub start_time: DateTime<Utc>,
    pub elapsed: Duration,
    pub value: serde_json::Value,
}

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
