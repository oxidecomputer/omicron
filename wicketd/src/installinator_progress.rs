// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tracker for installinator progress reports.
//!
//! This connects up the wicketd artifact server, which receives reports, to the
//! update tracker.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use installinator_artifactd::EventReportStatus;
use tokio::sync::{mpsc, oneshot};
use update_engine::events::StepEventIsTerminal;
use uuid::Uuid;

/// Creates the artifact server and update tracker's interfaces to the
/// installinator progress tracker.
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
    // Note: this is a std::sync::Mutex because it isn't held past an await
    // point. Tokio mutexes have cancel-safety issues.
    running_updates: Arc<Mutex<HashMap<Uuid, RunningUpdate>>>,
}

impl IprArtifactServer {
    pub(crate) fn report_progress(
        &self,
        update_id: Uuid,
        report: installinator_common::EventReport,
    ) -> EventReportStatus {
        let mut running_updates = self.running_updates.lock().unwrap();
        if let Some(update) = running_updates.get_mut(&update_id) {
            slog::debug!(
                self.log,
                "progress report seen ({} step events, {} progress events)",
                report.step_events.len(),
                report.progress_events.len();
                "update_id" => %update_id
            );
            // Note that take() leaves update in the Invalid state. Each branch
            // must restore *update to a valid state.
            match update.take() {
                RunningUpdate::Initial(start_sender) => {
                    slog::debug!(
                        self.log,
                        "first report seen for this update ID";
                        "update_id" => %update_id
                    );
                    let (sender, receiver) = mpsc::unbounded_channel();
                    _ = start_sender.send(receiver);
                    *update =
                        RunningUpdate::send_and_next_state(sender, report);
                }
                RunningUpdate::ReportsReceived(sender) => {
                    slog::debug!(
                        self.log,
                        "further report seen for this update ID";
                        "update_id" => %update_id
                    );
                    *update =
                        RunningUpdate::send_and_next_state(sender, report);
                }
                RunningUpdate::Closed => {
                    // The sender has been closed; ignore the report.
                    *update = RunningUpdate::Closed;
                }
                RunningUpdate::Invalid => {
                    unreachable!("invalid state")
                }
            }

            EventReportStatus::Processed
        } else {
            slog::debug!(self.log, "update ID unrecognized"; "update_id" => %update_id);
            EventReportStatus::UnrecognizedUpdateId
        }
    }
}

/// The update tracker's interface to the progress store.
#[derive(Clone, Debug)]
#[must_use]
pub struct IprUpdateTracker {
    log: slog::Logger,
    running_updates: Arc<Mutex<HashMap<Uuid, RunningUpdate>>>,
}

impl IprUpdateTracker {
    /// Registers an update ID and marks an update ID as started.
    ///
    /// Returns a oneshot receiver that resolves when the first message from the
    /// installinator has been received.
    ///
    /// Exposed for testing.
    #[doc(hidden)]
    pub fn register(&self, update_id: Uuid) -> IprStartReceiver {
        slog::debug!(self.log, "registering new update id"; "update_id" => %update_id);
        let (start_sender, start_receiver) = oneshot::channel();

        let mut running_updates = self.running_updates.lock().unwrap();
        running_updates.insert(update_id, RunningUpdate::Initial(start_sender));
        start_receiver
    }

    /// Returns the status of a running update, or None if the update ID hasn't
    /// been registered.
    pub fn update_state(&self, update_id: Uuid) -> Option<RunningUpdateState> {
        let running_updates = self.running_updates.lock().unwrap();
        running_updates.get(&update_id).map(|x| x.to_state())
    }
}

/// Type alias for the receiver that resolves when the first message from the
/// installinator has been received.
pub(crate) type IprStartReceiver = oneshot::Receiver<
    mpsc::UnboundedReceiver<installinator_common::EventReport>,
>;

#[derive(Debug)]
#[must_use]
enum RunningUpdate {
    /// This is the initial state: the first message from the installinator
    /// hasn't been received yet.
    Initial(
        oneshot::Sender<
            mpsc::UnboundedReceiver<installinator_common::EventReport>,
        >,
    ),

    /// Reports from the installinator have been received.
    ///
    /// This is an `UnboundedSender` to avoid cancel-safety issues (see
    /// https://github.com/oxidecomputer/omicron/pull/3579).
    ReportsReceived(mpsc::UnboundedSender<installinator_common::EventReport>),

    /// All messages have been received.
    ///
    /// We might receive multiple "completed" updates from the installinator in
    /// case there's a network issue between wicketd and installinator. Build in
    /// idempotency to ensure that the installinator doesn't fail for that
    /// reason: rather than removing the UUID from self.running_updates once
    /// it's done, we move to this state while keeping the UUID in the map.
    ///
    /// This does cause a memory leak since nothing comes along and cleans up
    /// entries in the Closed state yet, but we don't expect that to be a real
    /// issue, in part because it's very minor but also because an update will
    /// almost always cause a wicketd restart.
    Closed,

    /// Temporary state used to grab ownership of a running update.
    Invalid,
}

impl RunningUpdate {
    fn to_state(&self) -> RunningUpdateState {
        match self {
            RunningUpdate::Initial(_) => RunningUpdateState::Initial,
            RunningUpdate::ReportsReceived(_) => {
                RunningUpdateState::ReportsReceived
            }
            RunningUpdate::Closed => RunningUpdateState::Closed,
            RunningUpdate::Invalid => {
                unreachable!("invalid is a transient state")
            }
        }
    }
    fn take(&mut self) -> Self {
        std::mem::replace(self, Self::Invalid)
    }

    fn send_and_next_state(
        sender: mpsc::UnboundedSender<installinator_common::EventReport>,
        report: installinator_common::EventReport,
    ) -> Self {
        let is_terminal = Self::is_terminal(&report);
        _ = sender.send(report);
        if is_terminal {
            Self::Closed
        } else {
            Self::ReportsReceived(sender)
        }
    }

    fn is_terminal(report: &installinator_common::EventReport) -> bool {
        report
            .step_events
            .last()
            .map(|e| {
                matches!(
                    e.kind.is_terminal(),
                    StepEventIsTerminal::Terminal { .. }
                )
            })
            .unwrap_or(false)
    }
}

/// The current status of a running update.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunningUpdateState {
    /// The initial state: the first message from the installinator hasn't been
    /// received yet.
    Initial,

    /// Reports from the installinator have been received.
    ReportsReceived,

    /// All messages have been received.
    ///
    /// We might receive multiple "completed" updates from the installinator in
    /// case there's a network issue between wicketd and installinator. This
    /// state builds in idempotency to ensure that the installinator doesn't
    /// fail for that reason: rather than removing the UUID from the running
    /// update map once it's done, we move to this state while keeping the UUID
    /// in the map.
    Closed,
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use installinator_common::{
        InstallinatorCompletionMetadata, InstallinatorComponent,
        InstallinatorSpec, InstallinatorStepId, M2Slot, StepEvent,
        StepEventKind, StepInfo, StepInfoWithMetadata, StepOutcome,
        WriteOutput,
    };
    use omicron_test_utils::dev::test_setup_log;
    use schemars::JsonSchema;
    use update_engine::ExecutionId;

    use super::*;

    #[tokio::test]
    async fn test_states() {
        let logctx = test_setup_log("installinator_progress_test_states");

        let (ipr_artifact, ipr_update_tracker) = new(&logctx.log);

        let update_id = Uuid::new_v4();

        assert_eq!(
            ipr_artifact.report_progress(
                Uuid::new_v4(),
                installinator_common::EventReport::default()
            ),
            EventReportStatus::UnrecognizedUpdateId,
            "no registered UUIDs yet"
        );

        let mut start_receiver = ipr_update_tracker.register(update_id);

        assert_eq!(
            start_receiver.try_recv().unwrap_err(),
            oneshot::error::TryRecvError::Empty,
            "no progress yet"
        );

        let first_report = installinator_common::EventReport::default();

        assert_eq!(
            ipr_artifact.report_progress(update_id, first_report.clone()),
            EventReportStatus::Processed,
            "initial progress sent"
        );

        // There should now be progress.
        let mut receiver = start_receiver.await.expect("first progress seen");
        assert_eq!(
            receiver.recv().await,
            Some(first_report),
            "first report matches"
        );

        let execution_id = ExecutionId(Uuid::new_v4());

        // Send a completion report.
        let completion_report = installinator_common::EventReport {
            step_events: vec![StepEvent {
                spec: InstallinatorSpec::schema_name(),
                execution_id,
                event_index: 0,
                total_elapsed: Duration::from_secs(2),
                kind: StepEventKind::ExecutionCompleted {
                    last_step: StepInfoWithMetadata {
                        info: StepInfo {
                            id: InstallinatorStepId::Write,
                            component: InstallinatorComponent::Both,
                            description: "Fake step".into(),
                            index: 0,
                            component_index: 0,
                            total_component_steps: 1,
                        },
                        metadata: None,
                    },
                    last_attempt: 1,
                    last_outcome: StepOutcome::Success {
                        message: Some("Message".into()),
                        metadata: Some(
                            InstallinatorCompletionMetadata::Write {
                                output: WriteOutput {
                                    slots_attempted: vec![M2Slot::A, M2Slot::B]
                                        .into_iter()
                                        .collect(),
                                    slots_written: vec![M2Slot::A]
                                        .into_iter()
                                        .collect(),
                                },
                            },
                        ),
                    },
                    step_elapsed: Duration::from_secs(1),
                    attempt_elapsed: Duration::from_secs(1),
                },
            }],
            ..Default::default()
        };

        assert_eq!(
            ipr_artifact.report_progress(update_id, completion_report.clone()),
            EventReportStatus::Processed,
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
            ipr_artifact.report_progress(update_id, completion_report.clone()),
            EventReportStatus::Processed,
            "completion report sent after being closed"
        );

        logctx.cleanup_successful();
    }
}
