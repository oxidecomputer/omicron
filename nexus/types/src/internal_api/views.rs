// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::DateTime;
use chrono::Utc;
use futures::future::ready;
use futures::stream::StreamExt;
use omicron_common::api::external::ObjectStream;
use schemars::JsonSchema;
use serde::Serialize;
use std::time::Duration;
use std::time::Instant;
use steno::SagaResultErr;
use steno::UndoActionPermanentError;
use uuid::Uuid;

pub async fn to_list<T, U>(object_stream: ObjectStream<T>) -> Vec<U>
where
    T: Into<U>,
{
    object_stream
        .filter(|maybe_object| ready(maybe_object.is_ok()))
        .map(|maybe_object| maybe_object.unwrap().into())
        .collect::<Vec<U>>()
        .await
}

/// Sagas
///
/// These are currently only intended for observability by developers.  We will
/// eventually want to flesh this out into something more observable for end
/// users.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct Saga {
    pub id: Uuid,
    pub state: SagaState,
}

impl From<steno::SagaView> for Saga {
    fn from(s: steno::SagaView) -> Self {
        Saga { id: Uuid::from(s.id), state: SagaState::from(s.state) }
    }
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum SagaState {
    /// Saga is currently executing
    Running,
    /// Saga completed successfully
    Succeeded,
    /// One or more saga actions failed and the saga was successfully unwound
    /// (i.e., undo actions were executed for any actions that were completed).
    /// The saga is no longer running.
    Failed { error_node_name: steno::NodeName, error_info: SagaErrorInfo },
    /// One or more saga actions failed, *and* one or more undo actions failed
    /// during unwinding.  State managed by the saga may now be inconsistent.
    /// Support may be required to repair the state.  The saga is no longer
    /// running.
    Stuck {
        error_node_name: steno::NodeName,
        error_info: SagaErrorInfo,
        undo_error_node_name: steno::NodeName,
        undo_source_error: serde_json::Value,
    },
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(tag = "error", rename_all = "snake_case")]
pub enum SagaErrorInfo {
    ActionFailed { source_error: serde_json::Value },
    DeserializeFailed { message: String },
    InjectedError,
    SerializeFailed { message: String },
    SubsagaCreateFailed { message: String },
}

impl From<steno::ActionError> for SagaErrorInfo {
    fn from(error_source: steno::ActionError) -> Self {
        match error_source {
            steno::ActionError::ActionFailed { source_error } => {
                SagaErrorInfo::ActionFailed { source_error }
            }
            steno::ActionError::DeserializeFailed { message } => {
                SagaErrorInfo::DeserializeFailed { message }
            }
            steno::ActionError::InjectedError => SagaErrorInfo::InjectedError,
            steno::ActionError::SerializeFailed { message } => {
                SagaErrorInfo::SerializeFailed { message }
            }
            steno::ActionError::SubsagaCreateFailed { message } => {
                SagaErrorInfo::SubsagaCreateFailed { message }
            }
        }
    }
}

impl From<steno::SagaStateView> for SagaState {
    fn from(st: steno::SagaStateView) -> Self {
        match st {
            steno::SagaStateView::Ready { .. } => SagaState::Running,
            steno::SagaStateView::Running { .. } => SagaState::Running,
            steno::SagaStateView::Done {
                result: steno::SagaResult { kind: Ok(_), .. },
                ..
            } => SagaState::Succeeded,
            steno::SagaStateView::Done {
                result:
                    steno::SagaResult {
                        kind:
                            Err(SagaResultErr {
                                error_node_name,
                                error_source,
                                undo_failure: Some((undo_node_name, undo_error)),
                            }),
                        ..
                    },
                ..
            } => {
                // XXX-dap
                todo!();
                // let UndoActionPermanentError::PermanentFailure {
                //     source_error: undo_source_error,
                // } = undo_error;
                // SagaState::Stuck {
                //     error_node_name,
                //     error_info: SagaErrorInfo::from(error_source),
                //     undo_error_node_name: undo_node_name,
                //     undo_source_error,
                // }
            }
            steno::SagaStateView::Done {
                result:
                    steno::SagaResult {
                        kind:
                            Err(SagaResultErr {
                                error_node_name,
                                error_source,
                                undo_failure: None,
                            }),
                        ..
                    },
                ..
            } => SagaState::Failed {
                error_node_name,
                error_info: SagaErrorInfo::from(error_source),
            },
        }
    }
}

/// Background tasks
///
/// These are currently only intended for observability by developers.  We will
/// eventually want to flesh this out into something more observable for end
/// users.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct BackgroundTask {
    /// unique identifier for this background task
    name: String,
    /// brief summary (for developers) of what this task does
    description: String,
    /// how long after an activation completes before another will be triggered
    /// automatically
    ///
    /// (activations can also be triggered for other reasons)
    period: Duration,

    #[serde(flatten)]
    status: TaskStatus,
}

impl BackgroundTask {
    pub fn new(
        name: &str,
        description: &str,
        period: Duration,
        status: TaskStatus,
    ) -> BackgroundTask {
        BackgroundTask {
            name: name.to_owned(),
            description: description.to_owned(),
            period,
            status,
        }
    }
}

/// Describes why a background task was activated
///
/// This is only used for debugging.  This is deliberately not made available to
/// the background task itself.  See "Design notes" in the module-level
/// documentation for details.
#[derive(Debug, Clone, Copy, Eq, PartialEq, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivationReason {
    Signaled,
    Timeout,
    Dependency,
}

/// Describes the runtime status of the background task
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct TaskStatus {
    /// Describes the current task status
    pub current: CurrentStatus,
    /// Describes the last completed activation
    pub last: LastResult,
}

/// Describes the current status of a background task
#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case", tag = "current_status", content = "details")]
pub enum CurrentStatus {
    /// The background task is not running
    ///
    /// Typically, the task would be waiting for its next activation, which
    /// would happen after a timeout or some other event that triggers
    /// activation
    Idle,

    /// The background task is currently running
    ///
    /// More precisely, the task has been activated and has not yet finished
    /// this activation
    Running(CurrentStatusRunning),
}

impl CurrentStatus {
    pub fn is_idle(&self) -> bool {
        matches!(self, CurrentStatus::Idle)
    }

    pub fn unwrap_running(&self) -> &CurrentStatusRunning {
        match self {
            CurrentStatus::Running(r) => r,
            CurrentStatus::Idle => {
                panic!("attempted to get running state of idle task")
            }
        }
    }
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct CurrentStatusRunning {
    /// wall-clock time when the current activation started
    pub start_time: DateTime<Utc>,
    /// (local) monotonic timestamp when the activation started
    // Currently, it's not possible to read this value except in a debugger.
    // But it's still potentially useful to be able to read it in a debugger.
    #[allow(dead_code)]
    #[serde(skip)]
    pub start_instant: Instant,
    /// what kind of event triggered this activation
    pub reason: ActivationReason,
    /// which iteration this was (counter)
    pub iteration: u64,
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case", tag = "last_result", content = "details")]
pub enum LastResult {
    /// The task has never completed an activation
    NeverCompleted,
    /// The task has completed at least one activation
    Completed(LastResultCompleted),
}

impl LastResult {
    pub fn has_completed(&self) -> bool {
        matches!(self, LastResult::Completed(_))
    }

    pub fn unwrap_completion(self) -> LastResultCompleted {
        match self {
            LastResult::Completed(r) => r,
            LastResult::NeverCompleted => {
                panic!(
                    "attempted to get completion state of a task that \
                    has never completed"
                );
            }
        }
    }
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct LastResultCompleted {
    /// which iteration this was (counter)
    pub iteration: u64,
    /// wall-clock time when the activation started
    pub start_time: DateTime<Utc>,
    /// what kind of event triggered this activation
    pub reason: ActivationReason,
    /// total time elapsed during the activation
    pub elapsed: Duration,
    /// arbitrary datum emitted by the background task
    pub details: serde_json::Value,
}
