// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Common facilities for background tasks

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

// XXX-dap TODO-doc
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

        let log = opctx.log.new(o!("background_task" => name.clone()));
        let opctx = opctx.child(
            log,
            BTreeMap::from([("background_task".to_string(), name.clone())]),
        );
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
