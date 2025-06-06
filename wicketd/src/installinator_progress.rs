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

use installinator_api::EventReportStatus;
use omicron_uuid_kinds::MupdateUuid;
use tokio::sync::{oneshot, watch};
use update_engine::events::StepEventIsTerminal;

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
    running_updates: Arc<Mutex<HashMap<MupdateUuid, RunningUpdate>>>,
}

impl IprArtifactServer {
    pub(crate) fn report_progress(
        &self,
        update_id: MupdateUuid,
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
                    let is_terminal = RunningUpdate::is_terminal(&report);

                    let (sender, receiver) = watch::channel(report);
                    _ = start_sender.send(receiver);
                    // The first value was already sent above, so no need to
                    // call RunningUpdate::send_and_next_state. Just check
                    // is_terminal.
                    if is_terminal {
                        *update = RunningUpdate::Closed;
                    } else {
                        *update = RunningUpdate::ReportsReceived(sender);
                    }

                    EventReportStatus::Processed
                }
                RunningUpdate::ReportsReceived(sender) => {
                    slog::debug!(
                        self.log,
                        "further report seen for this update ID";
                        "update_id" => %update_id
                    );
                    let (new_state, ret) = RunningUpdate::send_and_next_state(
                        &self.log, sender, report,
                    );
                    *update = new_state;
                    ret
                }
                RunningUpdate::Closed => {
                    // The sender has been closed; ignore the report.
                    *update = RunningUpdate::Closed;
                    EventReportStatus::Processed
                }
                RunningUpdate::Invalid => {
                    unreachable!("invalid state")
                }
            }
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
    running_updates: Arc<Mutex<HashMap<MupdateUuid, RunningUpdate>>>,
}

impl IprUpdateTracker {
    /// Registers an update ID and marks an update ID as started.
    ///
    /// Returns a oneshot receiver that resolves when the first message from the
    /// installinator has been received.
    ///
    /// Exposed for testing.
    #[doc(hidden)]
    pub fn register(&self, update_id: MupdateUuid) -> IprStartReceiver {
        slog::debug!(self.log, "registering new update id"; "update_id" => %update_id);
        let (start_sender, start_receiver) = oneshot::channel();

        let mut running_updates = self.running_updates.lock().unwrap();
        running_updates.insert(update_id, RunningUpdate::Initial(start_sender));
        start_receiver
    }

    /// Returns the status of a running update, or None if the update ID hasn't
    /// been registered.
    pub fn update_state(
        &self,
        update_id: MupdateUuid,
    ) -> Option<RunningUpdateState> {
        let running_updates = self.running_updates.lock().unwrap();
        running_updates.get(&update_id).map(|x| x.to_state())
    }
}

/// Type alias for the receiver that resolves when the first message from the
/// installinator has been received.
pub(crate) type IprStartReceiver =
    oneshot::Receiver<watch::Receiver<installinator_common::EventReport>>;

#[derive(Debug)]
#[must_use]
enum RunningUpdate {
    /// This is the initial state: the first message from the installinator
    /// hasn't been received yet.
    Initial(
        oneshot::Sender<watch::Receiver<installinator_common::EventReport>>,
    ),

    /// Reports from the installinator have been received.
    ///
    /// This is an `UnboundedSender` to avoid cancel-safety issues (see
    /// <https://github.com/oxidecomputer/omicron/pull/3579>).
    ReportsReceived(watch::Sender<installinator_common::EventReport>),

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
        log: &slog::Logger,
        sender: watch::Sender<installinator_common::EventReport>,
        report: installinator_common::EventReport,
    ) -> (Self, EventReportStatus) {
        let is_terminal = Self::is_terminal(&report);
        match sender.send(report) {
            Ok(()) => {
                if is_terminal {
                    (Self::Closed, EventReportStatus::Processed)
                } else {
                    (
                        Self::ReportsReceived(sender),
                        EventReportStatus::Processed,
                    )
                }
            }
            Err(watch::error::SendError(report)) => {
                // This typically means that the receiver is closed (typically
                // due to the update process being aborted). If we enforced a
                // 1:1 relationship between installinator and wicketd, this
                // would indicate that the installinator should be aborted.
                //
                // Note that this "closed" is different from the Self::Closed
                // state -- the latter indicates that the installinator has sent
                // all of its messages.
                slog::warn!(
                    log,
                    "progress receiver is closed -- marking aborted";
                    "last_seen" => ?report.last_seen,
                );
                (
                    // Don't set the state to Closed here even if this is a
                    // terminal update.
                    //
                    // Why? Consider a situation where the receiver was closed
                    // right before the terminal update was received. Since the
                    // update was not delivered to the receiver, we need to keep
                    // communicating errors to the installinator. That is not
                    // what would happen with the Closed state.
                    //
                    // (This could also be done by storing a flag within the
                    // Closed state, but there's no real benefit to that
                    // approach.)
                    Self::ReportsReceived(sender),
                    EventReportStatus::ReceiverClosed,
                )
            }
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
        InstallinatorSpec, InstallinatorStepId, StepEvent, StepEventKind,
        StepInfo, StepInfoWithMetadata, StepOutcome, WriteOutput,
    };
    use omicron_common::disk::M2Slot;
    use omicron_test_utils::dev::test_setup_log;
    use schemars::JsonSchema;
    use update_engine::ExecutionId;
    use uuid::Uuid;

    use super::*;

    #[tokio::test]
    async fn test_states() {
        let logctx = test_setup_log("installinator_progress_test_states");

        let (ipr_artifact, ipr_update_tracker) = new(&logctx.log);

        let update_id = MupdateUuid::new_v4();

        assert_eq!(
            ipr_artifact.report_progress(
                MupdateUuid::new_v4(),
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
            *receiver.borrow_and_update(),
            first_report,
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
            *receiver.borrow_and_update(),
            completion_report,
            "completion report matches"
        );

        assert!(
            receiver.changed().await.is_err(),
            "sender closed after completion report"
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
