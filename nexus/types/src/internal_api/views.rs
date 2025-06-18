// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::deployment::PendingMgsUpdate;
use crate::inventory::BaseboardId;
use chrono::DateTime;
use chrono::SecondsFormat;
use chrono::Utc;
use futures::future::ready;
use futures::stream::StreamExt;
use nexus_sled_agent_shared::inventory::ConfigReconcilerInventory;
use nexus_sled_agent_shared::inventory::ConfigReconcilerInventoryResult;
use nexus_sled_agent_shared::inventory::OmicronZoneImageSource;
use nexus_sled_agent_shared::inventory::OmicronZoneType;
use omicron_common::api::external::MacAddr;
use omicron_common::api::external::ObjectStream;
use omicron_common::api::external::TufRepoDescription;
use omicron_common::api::external::Vni;
use omicron_common::snake_case_result;
use omicron_common::snake_case_result::SnakeCaseResult;
use omicron_uuid_kinds::DemoSagaUuid;
use omicron_uuid_kinds::{OmicronZoneUuid, SledUuid};
use schemars::JsonSchema;
use semver::Version;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::fmt::Display;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use steno::SagaResultErr;
use steno::UndoActionError;
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
                let UndoActionError::PermanentFailure {
                    source_error: undo_source_error,
                } = undo_error;
                SagaState::Stuck {
                    error_node_name,
                    error_info: SagaErrorInfo::from(error_source),
                    undo_error_node_name: undo_node_name,
                    undo_source_error,
                }
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

/// Identifies an instance of the demo saga
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct DemoSaga {
    pub saga_id: Uuid,
    pub demo_saga_id: DemoSagaUuid,
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

/// NAT Record
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct Ipv4NatEntryView {
    pub external_address: Ipv4Addr,
    pub first_port: u16,
    pub last_port: u16,
    pub sled_address: Ipv6Addr,
    pub vni: Vni,
    pub mac: MacAddr,
    pub gen: i64,
    pub deleted: bool,
}

// Externally-visible status for MGS-managed updates

/// Status of ongoing update attempts, recently completed attempts, and update
/// requests that are waiting for retry.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
pub struct MgsUpdateDriverStatus {
    pub recent: VecDeque<CompletedAttempt>,
    pub in_progress: BTreeMap<Arc<BaseboardId>, InProgressUpdateStatus>,
    pub waiting: BTreeMap<Arc<BaseboardId>, WaitingStatus>,
}

impl MgsUpdateDriverStatus {
    pub fn detailed_display(&self) -> MgsUpdateDriverStatusDisplay<'_> {
        MgsUpdateDriverStatusDisplay { status: self }
    }
}

pub struct MgsUpdateDriverStatusDisplay<'a> {
    status: &'a MgsUpdateDriverStatus,
}

impl Display for MgsUpdateDriverStatusDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let status = self.status;
        writeln!(f, "recent completed attempts:")?;
        for r in &status.recent {
            // Ignore units smaller than a millisecond.
            let elapsed = Duration::from_millis(
                u64::try_from(r.elapsed.as_millis()).unwrap_or(u64::MAX),
            );
            writeln!(
                f,
                "    {} to {} (took {}): serial {}",
                r.time_started.to_rfc3339_opts(SecondsFormat::Millis, true),
                r.time_done.to_rfc3339_opts(SecondsFormat::Millis, true),
                humantime::format_duration(elapsed),
                r.request.baseboard_id.serial_number,
            )?;
            writeln!(f, "        attempt#: {}", r.nattempts_done)?;
            writeln!(f, "        version:  {}", r.request.artifact_version)?;
            writeln!(f, "        hash:     {}", r.request.artifact_hash,)?;
            writeln!(f, "        result:   {:?}", r.result)?;
        }

        writeln!(f, "\ncurrently in progress:")?;
        for (baseboard_id, status) in &status.in_progress {
            // Ignore units smaller than a millisecond.
            let elapsed = Duration::from_millis(
                u64::try_from(
                    (status.time_started - Utc::now()).num_milliseconds(),
                )
                .unwrap_or(0),
            );
            writeln!(
                f,
                "    {}: serial {}: {:?} (attempt {}, running {})",
                status
                    .time_started
                    .to_rfc3339_opts(SecondsFormat::Millis, true),
                baseboard_id.serial_number,
                status.status,
                status.nattempts_done + 1,
                humantime::format_duration(elapsed),
            )?;
        }

        writeln!(f, "\nwaiting for retry:")?;
        for (baseboard_id, wait_info) in &status.waiting {
            writeln!(
                f,
                "    serial {}: will try again at {} (attempt {})",
                baseboard_id.serial_number,
                wait_info.next_attempt_time,
                wait_info.nattempts_done + 1,
            )?;
        }

        Ok(())
    }
}

/// externally-exposed status for a completed attempt
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct CompletedAttempt {
    pub time_started: DateTime<Utc>,
    pub time_done: DateTime<Utc>,
    pub elapsed: Duration,
    pub request: PendingMgsUpdate,
    #[serde(serialize_with = "snake_case_result::serialize")]
    #[schemars(
        schema_with = "SnakeCaseResult::<UpdateCompletedHow, String>::json_schema"
    )]
    pub result: Result<UpdateCompletedHow, String>,
    pub nattempts_done: u32,
}

#[derive(
    Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum UpdateCompletedHow {
    FoundNoChangesNeeded,
    CompletedUpdate,
    WaitedForConcurrentUpdate,
    TookOverConcurrentUpdate,
}

/// externally-exposed status for each in-progress update
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InProgressUpdateStatus {
    pub time_started: DateTime<Utc>,
    pub status: UpdateAttemptStatus,
    pub nattempts_done: u32,
}

/// status of a single update attempt
#[derive(
    Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum UpdateAttemptStatus {
    NotStarted,
    FetchingArtifact,
    Precheck,
    Updating,
    UpdateWaiting,
    PostUpdate,
    PostUpdateWait,
    Done,
}

/// externally-exposed status for waiting updates
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct WaitingStatus {
    pub next_attempt_time: DateTime<Utc>,
    pub nattempts_done: u32,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    JsonSchema,
)]
#[serde(
    rename_all = "snake_case",
    tag = "zone_status_version",
    content = "details"
)]
pub enum ZoneStatusVersion {
    Unknown,
    InstallDataset,
    Version(Version),
    Error(String),
}

impl Display for ZoneStatusVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ZoneStatusVersion::Unknown => write!(f, "unknown"),
            ZoneStatusVersion::InstallDataset => write!(f, "install dataset"),
            ZoneStatusVersion::Version(version) => {
                write!(f, "{}", version)
            }
            ZoneStatusVersion::Error(s) => {
                write!(f, "{}", s)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ZoneStatus {
    pub zone_id: OmicronZoneUuid,
    pub zone_type: OmicronZoneType,
    pub version: ZoneStatusVersion,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct UpdateStatus {
    pub zones: BTreeMap<SledUuid, Vec<ZoneStatus>>,
}

impl UpdateStatus {
    pub fn new<'a>(
        old: Option<&TufRepoDescription>,
        new: Option<&TufRepoDescription>,
        sleds: impl Iterator<
            Item = (&'a SledUuid, &'a Option<ConfigReconcilerInventory>),
        >,
    ) -> UpdateStatus {
        let zones = sleds
            .map(|(sled_id, inv)| {
                (
                    *sled_id,
                    inv.as_ref().map_or(vec![], |inv| {
                        inv.reconciled_omicron_zones()
                            .map(|(conf, res)| ZoneStatus {
                                zone_id: conf.id,
                                zone_type: conf.zone_type.clone(),
                                version: Self::zone_image_source_to_version(
                                    old,
                                    new,
                                    &conf.image_source,
                                    res,
                                ),
                            })
                            .collect()
                    }),
                )
            })
            .collect();
        UpdateStatus { zones }
    }

    pub fn zone_image_source_to_version(
        old: Option<&TufRepoDescription>,
        new: Option<&TufRepoDescription>,
        source: &OmicronZoneImageSource,
        res: &ConfigReconcilerInventoryResult,
    ) -> ZoneStatusVersion {
        if let ConfigReconcilerInventoryResult::Err { message } = res {
            return ZoneStatusVersion::Error(message.clone());
        }

        let &OmicronZoneImageSource::Artifact { hash } = source else {
            return ZoneStatusVersion::InstallDataset;
        };

        if let Some(old) = old {
            if let Some(_) = old.artifacts.iter().find(|meta| meta.hash == hash)
            {
                return ZoneStatusVersion::Version(
                    old.repo.system_version.clone(),
                );
            }
        }

        if let Some(new) = new {
            if let Some(_) = new.artifacts.iter().find(|meta| meta.hash == hash)
            {
                return ZoneStatusVersion::Version(
                    new.repo.system_version.clone(),
                );
            }
        }

        ZoneStatusVersion::Unknown
    }
}
