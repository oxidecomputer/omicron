// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::agent::Agent;
use super::agent::BootstrapError;
use super::http_entrypoints::RackInitializationStatus;
use super::http_entrypoints::SledResetStatus;
use super::params::RackInitializeRequest;
use futures::Future;
use std::fmt;
use std::future;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task::JoinError;
use tokio::task::JoinHandle;

pub(super) struct BootstrapContext {
    pub(super) agent: Arc<Agent>,
    pub(super) operation_interlock: SledOperationInterlock,
}

/// We expose some heavyweight, destructive operations in the bootstrap agent
/// http server: rack initialization, rack reset, and sled reset. None of those
/// operations should be allowed to run if another of them is still running. We
/// use this interlock to provide a both exclusivity and current status for any
/// of those operations.
pub(super) struct SledOperationInterlock {
    cmds_tx: mpsc::Sender<SledOperationInterlockCmd>,
    inner_task: JoinHandle<()>,
    rack_initialization_status_rx: watch::Receiver<RackInitializationStatus>,
    sled_reset_status_rx: watch::Receiver<SledResetStatus>,
}

impl Drop for SledOperationInterlock {
    fn drop(&mut self) {
        self.inner_task.abort();
    }
}

impl SledOperationInterlock {
    pub(super) fn new() -> Self {
        // Channel size is relatively arbitrary: we don't expect requests to sit
        // in here long (only long enough to either return an error or spawn a
        // task), so a small depth seems fine.
        let (cmds_tx, cmds_rx) = mpsc::channel(4);

        let (rack_initialization_status_tx, rack_initialization_status_rx) =
            watch::channel(RackInitializationStatus::NotRunning);
        let (sled_reset_status_tx, sled_reset_status_rx) =
            watch::channel(SledResetStatus::NotRunning);

        let inner = SledOperationInterlockInner {
            cmds_rx,
            running_task: None,
            rack_initialization_status_tx,
            sled_reset_status_tx,
        };

        let inner_task = tokio::spawn(inner.run());

        Self {
            cmds_tx,
            inner_task,
            rack_initialization_status_rx,
            sled_reset_status_rx,
        }
    }

    pub(crate) fn sled_reset_status(&self) -> SledResetStatus {
        self.sled_reset_status_rx.borrow().clone()
    }

    pub(crate) fn rack_initialization_status(
        &self,
    ) -> RackInitializationStatus {
        self.rack_initialization_status_rx.borrow().clone()
    }

    pub(crate) async fn rack_initialize(
        &self,
        agent: Arc<Agent>,
        request: RackInitializeRequest,
    ) -> Result<(), BootstrapError> {
        let (tx, rx) = oneshot::channel();

        // Our inner task is responsible for receiving this command and sending
        // us a response once it does; unwrapping here can only panic if that
        // task has itself panicked. Same applies to the other uses of `cmds_tx`
        // below.
        self.cmds_tx
            .send(SledOperationInterlockCmd::RackInitialize {
                agent,
                request,
                response: tx,
            })
            .await
            .unwrap();
        rx.await.unwrap()
    }

    pub(crate) async fn rack_reset(
        &self,
        agent: Arc<Agent>,
    ) -> Result<(), BootstrapError> {
        let (tx, rx) = oneshot::channel();
        self.cmds_tx
            .send(SledOperationInterlockCmd::RackReset { agent, response: tx })
            .await
            .unwrap();
        rx.await.unwrap()
    }

    pub(crate) async fn sled_reset(
        &self,
        agent: Arc<Agent>,
    ) -> Result<(), BootstrapError> {
        let (tx, rx) = oneshot::channel();
        self.cmds_tx
            .send(SledOperationInterlockCmd::SledReset { agent, response: tx })
            .await
            .unwrap();
        rx.await.unwrap()
    }
}

#[allow(clippy::large_enum_variant)]
enum SledOperationInterlockCmd {
    RackInitialize {
        agent: Arc<Agent>,
        request: RackInitializeRequest,
        response: oneshot::Sender<Result<(), BootstrapError>>,
    },
    RackReset {
        agent: Arc<Agent>,
        response: oneshot::Sender<Result<(), BootstrapError>>,
    },
    SledReset {
        agent: Arc<Agent>,
        response: oneshot::Sender<Result<(), BootstrapError>>,
    },
}

// `Agent` doesn't impl `Debug`, so we'll manually impl Debug on ourself.
impl fmt::Debug for SledOperationInterlockCmd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            SledOperationInterlockCmd::RackInitialize { .. } => {
                "SledOperationInterlockCmd::RackInitialize"
            }
            SledOperationInterlockCmd::RackReset { .. } => {
                "SledOperationInterlockCmd::RackReset"
            }
            SledOperationInterlockCmd::SledReset { .. } => {
                "SledOperationInterlockCmd::SledReset"
            }
        };
        f.write_str(s)
    }
}

type SledOperationTaskHandle = JoinHandle<Result<(), BootstrapError>>;
type SledOperationTaskResult = Result<Result<(), BootstrapError>, JoinError>;

enum SledOperationTask {
    RackInitializing(SledOperationTaskHandle),
    RackResetting(SledOperationTaskHandle),
    SledResetting(SledOperationTaskHandle),
}

struct SledOperationInterlockInner {
    cmds_rx: mpsc::Receiver<SledOperationInterlockCmd>,
    running_task: Option<SledOperationTask>,
    rack_initialization_status_tx: watch::Sender<RackInitializationStatus>,
    sled_reset_status_tx: watch::Sender<SledResetStatus>,
}

impl SledOperationInterlockInner {
    async fn run(mut self) {
        loop {
            // If we have a task running, we'll `select!` on its completion; if
            // not, we use `pending()` which gives back a future that never
            // completes.
            let mut pending = future::pending();
            let task_fut: &mut (dyn Future<Output = SledOperationTaskResult>
                      + Send
                      + Unpin) = match self.running_task.as_mut() {
                Some(
                    SledOperationTask::RackInitializing(t)
                    | SledOperationTask::RackResetting(t)
                    | SledOperationTask::SledResetting(t),
                ) => t,
                None => &mut pending,
            };

            tokio::select! {
                result = task_fut => {
                    self.handle_task_result(result);
                }

                Some(cmd) = self.cmds_rx.recv() => {
                    self.handle_cmd(cmd);
                }
            }
        }
    }

    fn handle_cmd(&mut self, cmd: SledOperationInterlockCmd) {
        fn send_busy_response(
            tx: oneshot::Sender<Result<(), BootstrapError>>,
            task: &SledOperationTask,
        ) {
            let message = match task {
                SledOperationTask::RackInitializing(_) => {
                    "rack busy initializing"
                }
                SledOperationTask::RackResetting(_) => "rack busy resetting",
                SledOperationTask::SledResetting(_) => "sled busy resetting",
            };
            _ = tx.send(Err(BootstrapError::ConcurrentSledOperationAccess {
                message,
            }));
        }

        match cmd {
            SledOperationInterlockCmd::RackInitialize {
                agent,
                request,
                response,
            } => {
                if let Some(task) = self.running_task.as_ref() {
                    send_busy_response(response, task);
                } else {
                    self.running_task = Some(
                        SledOperationTask::RackInitializing(tokio::spawn(
                            async move { agent.rack_initialize(request).await },
                        )),
                    );
                }
            }
            SledOperationInterlockCmd::RackReset { agent, response } => {
                if let Some(task) = self.running_task.as_ref() {
                    send_busy_response(response, task);
                } else {
                    self.running_task = Some(SledOperationTask::RackResetting(
                        tokio::spawn(async move { agent.rack_reset().await }),
                    ));
                }
            }
            SledOperationInterlockCmd::SledReset { agent, response } => {
                if let Some(task) = self.running_task.as_ref() {
                    send_busy_response(response, task);
                } else {
                    self.running_task = Some(SledOperationTask::SledResetting(
                        tokio::spawn(async move { agent.sled_reset().await }),
                    ));
                }
            }
        }
    }

    fn handle_task_result(&mut self, result: SledOperationTaskResult) {
        // We are called from `run()` when our currently-running task is
        // complete, which means there _must_ be a value in `self.running_task`
        // (otherwise `run()` couldn't have gotten a result at all!).
        let Some(running_task) = self.running_task.take() else {
            panic!("handle_task_result called with no running task?!");
        };

        match running_task {
            SledOperationTask::RackInitializing(_) => {
                // Ignore send errors
                _ = self.rack_initialization_status_tx.send(match result {
                    Ok(Ok(())) => RackInitializationStatus::Initialized,
                    Ok(Err(err)) => {
                        RackInitializationStatus::InitializationFailed {
                            reason: format!("{err:#}"),
                        }
                    }
                    Err(err) => {
                        let reason = if err.is_panic() {
                            "initialization task panicked"
                        } else if err.is_cancelled() {
                            "initialization task unexpectedly cancelled"
                        } else {
                            "initialization task failed (reason unknown)"
                        };
                        RackInitializationStatus::InitializationFailed {
                            reason: reason.into(),
                        }
                    }
                });
            }
            SledOperationTask::RackResetting(_) => {
                _ = self.rack_initialization_status_tx.send(match result {
                    Ok(Ok(())) => RackInitializationStatus::Reset,
                    Ok(Err(err)) => RackInitializationStatus::ResetFailed {
                        reason: format!("{err:#}"),
                    },
                    Err(err) => {
                        let reason = if err.is_panic() {
                            "reset task panicked"
                        } else if err.is_cancelled() {
                            "reset task unexpectedly cancelled"
                        } else {
                            "reset task failed (reason unknown)"
                        };
                        RackInitializationStatus::ResetFailed {
                            reason: reason.into(),
                        }
                    }
                });
            }
            SledOperationTask::SledResetting(_) => {
                _ = self.sled_reset_status_tx.send(match result {
                    Ok(Ok(())) => SledResetStatus::Reset,
                    Ok(Err(err)) => SledResetStatus::ResetFailed {
                        reason: format!("{err:#}"),
                    },
                    Err(err) => {
                        let reason = if err.is_panic() {
                            "reset task panicked"
                        } else if err.is_cancelled() {
                            "reset task unexpectedly cancelled"
                        } else {
                            "reset task failed (reason unknown)"
                        };
                        SledResetStatus::ResetFailed { reason: reason.into() }
                    }
                });
            }
        }
    }
}
