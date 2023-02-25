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
        log: log.new(
            slog::o!("component" => "installinator progress report manager"),
        ),
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
        slog::info!(self.log, "starting installinator progress report manager");
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
                    slog::info!(
                        self.log,
                        "report and register senders have been closed, exiting",
                    );
                    break;
                }
            }
        }
    }

    fn on_register_req(&mut self, req: RegisterReq) {
        slog::debug!(self.log, "registering request"; "update_id" => %req.update_id);

        let (start_sender, start_receiver) = oneshot::channel();
        self.running_updates
            .insert(req.update_id, RunningUpdate::Initial(start_sender));
        _ = req.resp.send(start_receiver);
    }

    async fn on_report_req(&mut self, req: ReportProgressReq) {
        if let Some(update) = self.running_updates.get_mut(&req.update_id) {
            slog::debug!(
                self.log,
                "progress report seen ({} completion events, {} progress events)",
                req.report.completion_events.len(),
                req.report.progress_events.len();
                "update_id" => %req.update_id
            );
            match update.take() {
                RunningUpdate::Initial(start_sender) => {
                    slog::debug!(
                        self.log,
                        "first report seen for this update ID";
                        "update_id" => %req.update_id
                    );
                    let (sender, receiver) = mpsc::channel(16);
                    _ = start_sender.send(receiver);
                    *update = RunningUpdate::send(sender, req.report).await;
                }
                RunningUpdate::ReportsReceived(sender) => {
                    slog::debug!(
                        self.log,
                        "further report seen for this update ID";
                        "update_id" => %req.update_id
                    );
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
            slog::debug!(self.log, "update ID unrecognized"; "update_id" => %req.update_id);
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
    ) -> Result<ProgressReportStatus, IprError> {
        let (resp, recv) = oneshot::channel();
        let req = ReportProgressReq { update_id, report, resp };
        self.report_sender
            .send(req)
            .await
            .map_err(|_| IprError::IprManagerDied)?;

        recv.await.map_err(|_| IprError::ResponseChannelDied)
    }
}

/// The update tracker's interface to the progress store.
#[derive(Debug)]
#[must_use]
pub(crate) struct IprUpdateTracker {
    register_sender: mpsc::Sender<RegisterReq>,
}

impl IprUpdateTracker {
    /// Registers an update ID and marks an update ID as started.
    ///
    /// Returns a oneshot receiver that resolves when the first message from the
    /// installinator has been received.
    pub(crate) async fn register(
        &self,
        update_id: Uuid,
    ) -> Result<IprStartReceiver, IprError> {
        // It is very important that this method receives an acknowledgment from
        // the manager that this UUID has been successfully registered,
        // otherwise this code is susceptible to a race condition. That's why we
        // create a oneshot channel to send the response.
        let (resp, recv) = oneshot::channel();
        let req = RegisterReq { update_id, resp };
        self.register_sender
            .send(req)
            .await
            .map_err(|_| IprError::IprManagerDied)?;
        recv.await.map_err(|_| IprError::ResponseChannelDied)
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
    resp: oneshot::Sender<IprStartReceiver>,
}

#[derive(Debug, Clone, Error)]
pub(crate) enum IprError {
    #[error("progress report manager died")]
    IprManagerDied,

    #[error("response channel died")]
    ResponseChannelDied,
}

impl IprError {
    pub(crate) fn to_http_error(&self) -> HttpError {
        let message = DisplayErrorChain::new(self).to_string();

        match self {
            IprError::IprManagerDied | IprError::ResponseChannelDied => {
                HttpError::for_unavail(None, message)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use installinator_common::CompletionEvent;
    use omicron_test_utils::dev::test_setup_log;

    use super::*;

    #[tokio::test]
    async fn test_states() {
        let logctx = test_setup_log("installinator_progress_test_states");

        let (ipr_manager, ipr_artifact, ipr_update_tracker) = new(&logctx.log);
        tokio::spawn(async move { ipr_manager.run().await });

        let update_id = Uuid::new_v4();

        assert_eq!(
            ipr_artifact
                .report_progress(Uuid::new_v4(), ProgressReport::default())
                .await
                .unwrap(),
            ProgressReportStatus::UnrecognizedUpdateId,
            "no registered UUIDs yet"
        );

        let mut start_receiver =
            ipr_update_tracker.register(update_id).await.unwrap();

        assert_eq!(
            start_receiver.try_recv().unwrap_err(),
            oneshot::error::TryRecvError::Empty,
            "no progress yet"
        );

        let first_report = ProgressReport {
            total_elapsed: Duration::from_secs(1),
            ..Default::default()
        };

        assert_eq!(
            ipr_artifact
                .report_progress(update_id, first_report.clone())
                .await
                .unwrap(),
            ProgressReportStatus::Processed,
            "initial progress sent"
        );

        // There should now be progress.
        let mut receiver = start_receiver.await.expect("first progress seen");
        assert_eq!(
            receiver.recv().await,
            Some(first_report),
            "first report matches"
        );

        // Send a completion report.
        let completion_report = ProgressReport {
            total_elapsed: Duration::from_secs(4),
            completion_events: vec![CompletionEvent {
                total_elapsed: Duration::from_secs(2),
                kind: CompletionEventKind::Completed,
            }],
            ..Default::default()
        };

        assert_eq!(
            ipr_artifact
                .report_progress(update_id, completion_report.clone())
                .await
                .unwrap(),
            ProgressReportStatus::Processed,
            "completion report sent"
        );

        assert_eq!(
            receiver.recv().await,
            Some(completion_report.clone()),
            "completion report matches"
        );

        assert_eq!(
            receiver.recv().await,
            None,
            "receiver closed after completion report"
        );

        // Try sending the completion report again (simulating a situation where
        // wicketd processed the message but the installinator didn't receive
        // it, causing the installinator to try again). This should result in
        // another Processed message rather than UnrecognizedUpdateId.
        assert_eq!(
            ipr_artifact
                .report_progress(update_id, completion_report.clone())
                .await
                .unwrap(),
            ProgressReportStatus::Processed,
            "completion report sent after being closed"
        );

        logctx.cleanup_successful();
    }
}
