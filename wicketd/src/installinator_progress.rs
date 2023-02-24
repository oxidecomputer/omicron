// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tracker for installinator progress reports.
//!
//! This connects up the wicketd artifact server, which receives reports, to the
//! update tracker.

use std::collections::HashMap;

use display_error_chain::DisplayErrorChain;
use dropshot::HttpError;
use installinator_artifactd::ProgressReportStatus;
use installinator_common::{CompletionEventKind, ProgressReport};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

/// Creates an [`IprManager`] and the artifact server and update tracker's interfaces to it.
pub(crate) fn new(
    log: &slog::Logger,
) -> (IprManager, IprArtifactServer, IprUpdateTracker) {
    let running_updates = HashMap::new();
    let (register_sender, register_receiver) = mpsc::channel(16);
    let (report_sender, report_receiver) = mpsc::channel(16);

    let ipr_manager = IprManager {
        log: log.new(slog::o!("component" => "InstallinatorTracker")),
        running_updates,
        register_receiver,
        report_receiver,
    };
    let ipr_artifact = IprArtifactServer { report_sender };
    let ipr_update_tracker = IprUpdateTracker { register_sender };

    (ipr_manager, ipr_artifact, ipr_update_tracker)
}

#[derive(Debug)]
pub(crate) struct IprManager {
    log: slog::Logger,
    running_updates: HashMap<Uuid, RunningUpdate>,
    register_receiver: mpsc::Receiver<RegisterReq>,
    report_receiver: mpsc::Receiver<ReportProgressReq>,
}

impl IprManager {
    pub(crate) async fn run(mut self) {
        slog::debug!(self.log, "starting installinator tracker");
        let mut register_done = false;
        let mut report_done = false;

        loop {
            tokio::select! {
                req = self.register_receiver.recv(), if !register_done => {
                    if let Some(req) = req {
                        self.on_register_req(req);
                    } else {
                        register_done = true;
                    }
                }
                req = self.report_receiver.recv(), if !report_done => {
                    if let Some(req) = req {
                        self.on_report_req(req).await;
                    } else {
                        report_done = true;
                    }
                }
                else => {
                    break;
                }
            }
        }
    }

    fn on_register_req(&mut self, req: RegisterReq) {
        slog::debug!(self.log, "registering"; "update_id" => %req.update_id);
        self.running_updates
            .insert(req.update_id, RunningUpdate::Initial(req.sender));
    }

    async fn on_report_req(&mut self, req: ReportProgressReq) {
        slog::debug!(self.log, "received progress report"; "update_id" => %req.update_id);
        if let Some(update) = self.running_updates.get_mut(&req.update_id) {
            match update.take() {
                RunningUpdate::Initial(start_sender) => {
                    // Let the sender know that the receiver has been closed.
                    let (sender, receiver) = mpsc::channel(16);
                    _ = start_sender.send(receiver);
                    *update = RunningUpdate::send(sender, req.report).await;
                }
                RunningUpdate::ReportsReceived(sender) => {
                    *update = RunningUpdate::send(sender, req.report).await;
                }
                RunningUpdate::Closed => {
                    // The sender has been closed; ignore the report.
                    *update = RunningUpdate::Closed;
                }
                RunningUpdate::Invalid => {
                    unreachable!("invalid state")
                }
            }

            _ = req.resp.send(ProgressReportStatus::Processed);
        } else {
            _ = req.resp.send(ProgressReportStatus::UnrecognizedUpdateId);
        }
    }
}

/// The artifact server's interface to the installinator tracker.
#[derive(Debug)]
#[must_use]
pub(crate) struct IprArtifactServer {
    report_sender: mpsc::Sender<ReportProgressReq>,
}

impl IprArtifactServer {
    pub(crate) async fn report_progress(
        &self,
        update_id: Uuid,
        report: ProgressReport,
    ) -> Result<ProgressReportStatus, ProgressError> {
        let (resp, recv) = oneshot::channel();
        let req = ReportProgressReq { update_id, report, resp };
        self.report_sender
            .send(req)
            .await
            .map_err(|_| ProgressError::ProgressStoreTaskDead)?;

        recv.await.map_err(|_| ProgressError::ProgressStoreTaskDead)
    }
}

/// The update tracker's interface to the progress store.
#[derive(Debug)]
#[must_use]
pub(crate) struct IprUpdateTracker {
    register_sender: mpsc::Sender<RegisterReq>,
}

impl IprUpdateTracker {
    /// Registers an update ID and marks an update ID as started, returning a
    /// oneshot receiver that resolves when the first message from the
    /// installinator has been received.
    pub(crate) async fn register(&self, update_id: Uuid) -> IprStartReceiver {
        let (sender, receiver) = oneshot::channel();
        let req = RegisterReq { update_id, sender };
        _ = self.register_sender.send(req).await;
        receiver
    }
}

/// Type alias for the receiver that resolves when the first message from the
/// installinator has been received.
pub(crate) type IprStartReceiver =
    oneshot::Receiver<mpsc::Receiver<ProgressReport>>;

#[derive(Debug)]
#[must_use]
enum RunningUpdate {
    /// This is the initial state: the first message from the installinator
    /// hasn't been received yet.
    Initial(oneshot::Sender<mpsc::Receiver<ProgressReport>>),

    /// Reports from the installinator have been received.
    ReportsReceived(mpsc::Sender<ProgressReport>),

    /// All messages have been received.
    ///
    /// We might receive multiple "completed" updates from the installinator in
    /// case there's a network issue between wicketd and installinator. Build in
    /// idempotency to ensure that the installinator doesn't fail for that
    /// reason: rather than removing the UUID from self.running_updates once
    /// it's done, we move to this state while keeping the UUID in the map.
    Closed,

    /// Temporary state used for on_report. Invalid outside on_report.
    Invalid,
}

impl RunningUpdate {
    fn take(&mut self) -> Self {
        // Temporarily move to this state
        std::mem::replace(self, Self::Invalid)
    }

    async fn send(
        sender: mpsc::Sender<ProgressReport>,
        report: ProgressReport,
    ) -> Self {
        let is_completed = Self::is_completed(&report);
        _ = sender.send(report).await;
        if is_completed {
            Self::Closed
        } else {
            Self::ReportsReceived(sender)
        }
    }

    fn is_completed(report: &ProgressReport) -> bool {
        report
            .completion_events
            .iter()
            // Scan in reverse order since the completion message is very likely
            // to be the last one.
            .rev()
            .any(|event| matches!(event.kind, CompletionEventKind::Completed))
    }
}

/// An individual request to report progress, with a response channel.
#[derive(Debug)]
struct ReportProgressReq {
    update_id: Uuid,
    report: ProgressReport,
    resp: oneshot::Sender<ProgressReportStatus>,
}

#[derive(Debug)]
struct RegisterReq {
    update_id: Uuid,
    sender: oneshot::Sender<mpsc::Receiver<ProgressReport>>,
}

#[derive(Debug, Error)]
pub(crate) enum ProgressError {
    #[error("progress store task is dead")]
    ProgressStoreTaskDead,
}

impl ProgressError {
    pub(crate) fn to_http_error(&self) -> HttpError {
        let message = DisplayErrorChain::new(self).to_string();

        match self {
            ProgressError::ProgressStoreTaskDead => {
                HttpError::for_unavail(None, message)
            }
        }
    }
}
