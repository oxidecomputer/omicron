// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tracker for installinator progress reports.
//!
//! This connects up the wicketd artifact server, which receives reports, to the
//! update tracker.

use std::{collections::HashMap, sync::Arc};

use installinator_artifactd::ProgressReportStatus;
use installinator_common::{CompletionEventKind, ProgressReport};
use tokio::sync::{mpsc, oneshot, Mutex};
use uuid::Uuid;

/// Creates an [`IprManager`] and the artifact server and update tracker's interfaces to it.
pub(crate) fn new(log: &slog::Logger) -> (IprArtifactServer, IprUpdateTracker) {
    let running_updates = Arc::new(Mutex::new(HashMap::new()));

    let ipr_artifact = IprArtifactServer {
        log: log.new(
            slog::o!("component" => "installinator progress reports, artifact server"),
        ),
        running_updates: running_updates.clone(),
    };
    let ipr_update_tracker = IprUpdateTracker { 
        log: log.new(
            slog::o!("component" => "installinator progress reports, update tracker"),
        ),
        running_updates,
    };

    (ipr_artifact, ipr_update_tracker)
}

/// The artifact server's interface to the installinator tracker.
#[derive(Debug)]
#[must_use]
pub(crate) struct IprArtifactServer {
    log: slog::Logger,
    running_updates: Arc<Mutex<HashMap<Uuid, RunningUpdate>>>,
}

impl IprArtifactServer {
    pub(crate) async fn report_progress(
        &self,
        update_id: Uuid,
        report: ProgressReport,
    ) -> ProgressReportStatus {
        let mut running_updates = self.running_updates.lock().await;
        if let Some(update) = running_updates.get_mut(&update_id) {
            slog::debug!(
                self.log,
                "progress report seen ({} completion events, {} progress events)",
                report.completion_events.len(),
                report.progress_events.len();
                "update_id" => %update_id
            );
            match update.take() {
                RunningUpdate::Initial(start_sender) => {
                    slog::debug!(
                        self.log,
                        "first report seen for this update ID";
                        "update_id" => %update_id
                    );
                    let (sender, receiver) = mpsc::channel(16);
                    _ = start_sender.send(receiver);
                    *update = RunningUpdate::send_and_next_state(sender, report).await;
                }
                RunningUpdate::ReportsReceived(sender) => {
                    slog::debug!(
                        self.log,
                        "further report seen for this update ID";
                        "update_id" => %update_id
                    );
                    *update = RunningUpdate::send_and_next_state(sender, report).await;
                }
                RunningUpdate::Closed => {
                    // The sender has been closed; ignore the report.
                    *update = RunningUpdate::Closed;
                }
                RunningUpdate::Invalid => {
                    unreachable!("invalid state")
                }
            }

            ProgressReportStatus::Processed
        } else {
            slog::debug!(self.log, "update ID unrecognized"; "update_id" => %update_id);
            ProgressReportStatus::UnrecognizedUpdateId
        }
    }
}

/// The update tracker's interface to the progress store.
#[derive(Debug)]
#[must_use]
pub(crate) struct IprUpdateTracker {
    log: slog::Logger,
    running_updates: Arc<Mutex<HashMap<Uuid, RunningUpdate>>>,
}

impl IprUpdateTracker {
    /// Registers an update ID and marks an update ID as started.
    ///
    /// Returns a oneshot receiver that resolves when the first message from the
    /// installinator has been received.
    pub(crate) async fn register(&self, update_id: Uuid) -> IprStartReceiver {
        slog::debug!(self.log, "registering new update id"; "update_id" => %update_id);
        let (start_sender, start_receiver) = oneshot::channel();

        let mut running_updates = self.running_updates.lock().await;
        running_updates.insert(update_id, RunningUpdate::Initial(start_sender));
        start_receiver
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

    async fn send_and_next_state(
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use installinator_common::CompletionEvent;
    use omicron_test_utils::dev::test_setup_log;

    use super::*;

    #[tokio::test]
    async fn test_states() {
        let logctx = test_setup_log("installinator_progress_test_states");

        let (ipr_artifact, ipr_update_tracker) = new(&logctx.log);

        let update_id = Uuid::new_v4();

        assert_eq!(
            ipr_artifact
                .report_progress(Uuid::new_v4(), ProgressReport::default())
                .await,
            ProgressReportStatus::UnrecognizedUpdateId,
            "no registered UUIDs yet"
        );

        let mut start_receiver = ipr_update_tracker.register(update_id).await;

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
            ipr_artifact.report_progress(update_id, first_report.clone()).await,
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
                .await,
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
                .await,
            ProgressReportStatus::Processed,
            "completion report sent after being closed"
        );

        logctx.cleanup_successful();
    }
}
