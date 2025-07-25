// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::deployment::PendingMgsUpdate;
use crate::deployment::TargetReleaseDescription;
use crate::inventory::BaseboardId;
use crate::inventory::Caboose;
use crate::inventory::CabooseWhich;
use crate::inventory::Collection;
use chrono::DateTime;
use chrono::SecondsFormat;
use chrono::Utc;
use futures::future::ready;
use futures::stream::StreamExt;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use nexus_sled_agent_shared::inventory::ConfigReconcilerInventoryResult;
use nexus_sled_agent_shared::inventory::OmicronZoneImageSource;
use nexus_sled_agent_shared::inventory::OmicronZoneType;
use omicron_common::api::external::MacAddr;
use omicron_common::api::external::ObjectStream;
use omicron_common::api::external::TufArtifactMeta;
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
use tufaceous_artifact::KnownArtifactKind;
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
#[derive(
    Clone, Debug, Default, Eq, PartialEq, Deserialize, Serialize, JsonSchema,
)]
pub struct MgsUpdateDriverStatus {
    pub recent: VecDeque<CompletedAttempt>,
    pub in_progress: IdOrdMap<InProgressUpdateStatus>,
    pub waiting: IdOrdMap<WaitingStatus>,
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
        for status in &status.in_progress {
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
                status.baseboard_id.serial_number,
                status.status,
                status.nattempts_done + 1,
                humantime::format_duration(elapsed),
            )?;
        }

        writeln!(f, "\nwaiting for retry:")?;
        for wait_info in &status.waiting {
            writeln!(
                f,
                "    serial {}: will try again at {} (attempt {})",
                wait_info.baseboard_id.serial_number,
                wait_info.next_attempt_time,
                wait_info.nattempts_done + 1,
            )?;
        }

        Ok(())
    }
}

/// externally-exposed status for a completed attempt
#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
pub struct CompletedAttempt {
    pub time_started: DateTime<Utc>,
    pub time_done: DateTime<Utc>,
    pub elapsed: Duration,
    pub request: PendingMgsUpdate,
    #[serde(with = "snake_case_result")]
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
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
pub struct InProgressUpdateStatus {
    pub baseboard_id: Arc<BaseboardId>,
    pub time_started: DateTime<Utc>,
    pub status: UpdateAttemptStatus,
    pub nattempts_done: u32,
}

impl IdOrdItem for InProgressUpdateStatus {
    type Key<'a> = &'a BaseboardId;

    fn key(&self) -> Self::Key<'_> {
        &self.baseboard_id
    }

    id_upcast!();
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
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
pub struct WaitingStatus {
    pub baseboard_id: Arc<BaseboardId>,
    pub next_attempt_time: DateTime<Utc>,
    pub nattempts_done: u32,
}

impl IdOrdItem for WaitingStatus {
    type Key<'a> = &'a BaseboardId;

    fn key(&self) -> Self::Key<'_> {
        &self.baseboard_id
    }

    id_upcast!();
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
pub enum TufRepoVersion {
    Unknown,
    InstallDataset,
    Version(Version),
    Error(String),
}

impl Display for TufRepoVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TufRepoVersion::Unknown => write!(f, "unknown"),
            TufRepoVersion::InstallDataset => {
                write!(f, "install dataset")
            }
            TufRepoVersion::Version(version) => {
                write!(f, "{}", version)
            }
            TufRepoVersion::Error(s) => {
                write!(f, "{}", s)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ZoneStatus {
    pub zone_id: OmicronZoneUuid,
    pub zone_type: OmicronZoneType,
    pub version: TufRepoVersion,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SpStatus {
    pub sled_id: Option<SledUuid>,
    pub slot0_version: TufRepoVersion,
    pub slot1_version: TufRepoVersion,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct UpdateStatus {
    pub zones: BTreeMap<SledUuid, Vec<ZoneStatus>>,
    pub sps: BTreeMap<String, SpStatus>,
}

impl UpdateStatus {
    pub fn new(
        old: &TargetReleaseDescription,
        new: &TargetReleaseDescription,
        inventory: &Collection,
    ) -> UpdateStatus {
        let sleds = inventory
            .sled_agents
            .iter()
            .map(|agent| (&agent.sled_id, &agent.last_reconciliation));
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
        let baseboard_ids: Vec<_> = inventory.sps.keys().cloned().collect();

        // Find all SP versions and git commits via cabooses
        let mut sps: BTreeMap<BaseboardId, SpStatus> = baseboard_ids
            .into_iter()
            .map(|baseboard_id| {
                let slot0_version = inventory
                    .caboose_for(CabooseWhich::SpSlot0, &baseboard_id)
                    .map_or(TufRepoVersion::Unknown, |c| {
                        Self::caboose_to_version(old, new, &c.caboose)
                    });
                let slot1_version = inventory
                    .caboose_for(CabooseWhich::SpSlot1, &baseboard_id)
                    .map_or(TufRepoVersion::Unknown, |c| {
                        Self::caboose_to_version(old, new, &c.caboose)
                    });
                (
                    (*baseboard_id).clone(),
                    SpStatus { sled_id: None, slot0_version, slot1_version },
                )
            })
            .collect();

        // Fill in the sled_id for the sp if known
        for sa in inventory.sled_agents.iter() {
            if let Some(baseboard_id) = &sa.baseboard_id {
                if let Some(sp) = sps.get_mut(baseboard_id) {
                    sp.sled_id = Some(sa.sled_id);
                }
            }
        }

        let sps = sps.into_iter().map(|(k, v)| (k.to_string(), v)).collect();

        UpdateStatus { zones, sps }
    }

    fn caboose_to_version(
        old: &TargetReleaseDescription,
        new: &TargetReleaseDescription,
        caboose: &Caboose,
    ) -> TufRepoVersion {
        let matching_caboose = |a: &TufArtifactMeta| {
            caboose.board == a.id.name
                && matches!(
                    a.id.kind.to_known(),
                    Some(
                        KnownArtifactKind::GimletSp
                            | KnownArtifactKind::PscSp
                            | KnownArtifactKind::SwitchSp
                    )
                )
                && caboose.version == a.id.version.to_string()
        };
        if let Some(new) = new.tuf_repo() {
            if new.artifacts.iter().any(matching_caboose) {
                return TufRepoVersion::Version(
                    new.repo.system_version.clone(),
                );
            }
        }
        if let Some(old) = old.tuf_repo() {
            if old.artifacts.iter().any(matching_caboose) {
                return TufRepoVersion::Version(
                    old.repo.system_version.clone(),
                );
            }
        }

        TufRepoVersion::Unknown
    }

    fn zone_image_source_to_version(
        old: &TargetReleaseDescription,
        new: &TargetReleaseDescription,
        source: &OmicronZoneImageSource,
        res: &ConfigReconcilerInventoryResult,
    ) -> TufRepoVersion {
        if let ConfigReconcilerInventoryResult::Err { message } = res {
            return TufRepoVersion::Error(message.clone());
        }

        let &OmicronZoneImageSource::Artifact { hash } = source else {
            return TufRepoVersion::InstallDataset;
        };

        if let Some(new) = new.tuf_repo() {
            if new.artifacts.iter().any(|meta| meta.hash == hash) {
                return TufRepoVersion::Version(
                    new.repo.system_version.clone(),
                );
            }
        }

        if let Some(old) = old.tuf_repo() {
            if old.artifacts.iter().any(|meta| meta.hash == hash) {
                return TufRepoVersion::Version(
                    old.repo.system_version.clone(),
                );
            }
        }

        TufRepoVersion::Unknown
    }
}

// XXX-dap TODO-doc
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct QuiesceStatus {
    pub state: QuiesceState,
    pub sagas_running: IdOrdMap<RunningSagaInfo>,
}

// XXX-dap TODO-doc
#[derive(Debug, Clone, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "state", content = "quiesce_details")]
pub enum QuiesceState {
    /// Normal operation
    Running,
    /// New sagas disallowed, but some may be running.
    WaitingForSagas {
        time_requested: DateTime<Utc>,
        #[serde(skip)]
        time_waiting_for_sagas: Instant,
    },
    /// No sagas running, but some database connections may be held.
    WaitingForDb {
        time_requested: DateTime<Utc>,
        #[serde(skip)]
        time_waiting_for_sagas: Instant,
        duration_waiting_for_sagas: Duration,
        #[serde(skip)]
        time_waiting_for_db: Instant,
    },
    /// Nexus has no sagas running and is not using the database
    Quiesced {
        time_requested: DateTime<Utc>,
        time_quiesced: DateTime<Utc>,
        duration_waiting_for_sagas: Duration,
        duration_waiting_for_db: Duration,
        duration_total: Duration,
    },
}

impl QuiesceState {
    pub fn running() -> QuiesceState {
        QuiesceState::Running
    }

    pub fn quiescing(&self) -> bool {
        match self {
            QuiesceState::Running => false,
            QuiesceState::WaitingForSagas { .. }
            | QuiesceState::WaitingForDb { .. }
            | QuiesceState::Quiesced { .. } => true,
        }
    }

    pub fn fully_quiesced(&self) -> bool {
        match self {
            QuiesceState::Running
            | QuiesceState::WaitingForSagas { .. }
            | QuiesceState::WaitingForDb { .. } => false,
            QuiesceState::Quiesced { .. } => true,
        }
    }
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct RunningSagaInfo {
    pub saga_id: steno::SagaId,
    pub saga_name: steno::SagaName,
    pub time_started: DateTime<Utc>,
}

impl IdOrdItem for RunningSagaInfo {
    type Key<'a> = &'a steno::SagaId;

    fn key(&self) -> Self::Key<'_> {
        &self.saga_id
    }

    id_upcast!();
}

#[cfg(test)]
mod test {
    use super::CompletedAttempt;
    use super::InProgressUpdateStatus;
    use super::MgsUpdateDriverStatus;
    use super::UpdateCompletedHow;
    use super::WaitingStatus;
    use crate::deployment::ExpectedVersion;
    use crate::deployment::PendingMgsUpdate;
    use crate::deployment::PendingMgsUpdateDetails;
    use crate::internal_api::views::UpdateAttemptStatus;
    use crate::inventory::BaseboardId;
    use chrono::Utc;
    use gateway_client::types::SpType;
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::time::Instant;
    use tufaceous_artifact::ArtifactHash;

    #[test]
    fn test_can_serialize_mgs_updates() {
        let start = Instant::now();
        let artifact_hash: ArtifactHash =
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                .parse()
                .unwrap();
        let baseboard_id = Arc::new(BaseboardId {
            part_number: String::from("a_port"),
            serial_number: String::from("a_serial"),
        });
        let waiting = WaitingStatus {
            baseboard_id: baseboard_id.clone(),
            next_attempt_time: Utc::now(),
            nattempts_done: 2,
        };
        let in_progress = InProgressUpdateStatus {
            baseboard_id: baseboard_id.clone(),
            time_started: Utc::now(),
            status: UpdateAttemptStatus::Updating,
            nattempts_done: 3,
        };
        let completed = CompletedAttempt {
            time_started: Utc::now(),
            time_done: Utc::now(),
            elapsed: start.elapsed(),
            request: PendingMgsUpdate {
                baseboard_id: baseboard_id.clone(),
                sp_type: SpType::Sled,
                slot_id: 12,
                details: PendingMgsUpdateDetails::Sp {
                    expected_active_version: "1.0.0".parse().unwrap(),
                    expected_inactive_version: ExpectedVersion::NoValidVersion,
                },
                artifact_hash,
                artifact_version: "2.0.0".parse().unwrap(),
            },
            result: Ok(UpdateCompletedHow::FoundNoChangesNeeded),
            nattempts_done: 8,
        };

        let status = MgsUpdateDriverStatus {
            recent: VecDeque::from([completed]),
            in_progress: std::iter::once(in_progress).collect(),
            waiting: std::iter::once(waiting).collect(),
        };

        let serialized =
            serde_json::to_string(&status).expect("failed to serialize value");
        let deserialized: MgsUpdateDriverStatus =
            serde_json::from_str(&serialized)
                .expect("failed to deserialize value");
        assert_eq!(deserialized, status);
    }
}
