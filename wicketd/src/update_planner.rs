// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use crate::artifacts::WicketdArtifactStore;
use crate::mgs::make_mgs_client;
use crate::update_events::UpdateEventFailureKind;
use crate::update_events::UpdateEventKind;
use crate::update_events::UpdateEventSuccessKind;
use crate::update_events::UpdateStateKind;
use anyhow::bail;
use anyhow::ensure;
use anyhow::Context;
use buf_list::BufList;
use debug_ignore::DebugIgnore;
use gateway_client::types::SpIdentifier;
use gateway_client::types::SpType;
use gateway_client::types::SpUpdateStatus;
use gateway_client::types::UpdateBody;
use gateway_messages::SpComponent;
use omicron_common::api::internal::nexus::KnownArtifactKind;
use omicron_common::update::ArtifactId;
use slog::error;
use slog::info;
use slog::o;
use slog::Logger;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::net::SocketAddrV6;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::Duration;
use std::time::Instant;
use thiserror::Error;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use uuid::Uuid;

// These three types are mirrors of the HTTP
// `UpdateState`/`UpdateEvent`/`UpdateLog`, but with the timestamps stored as
// `Instant`s. This allows us to convert them to `Duration`s (i.e., ages) when
// returning them to a caller.
#[derive(Clone, Debug)]
struct UpdateState {
    timestamp: Instant,
    kind: UpdateStateKind,
}

impl From<UpdateState> for crate::update_events::UpdateState {
    fn from(state: UpdateState) -> Self {
        Self { age: state.timestamp.elapsed(), kind: state.kind }
    }
}

#[derive(Clone, Debug)]
struct UpdateEvent {
    timestamp: Instant,
    kind: UpdateEventKind,
}

impl From<UpdateEvent> for crate::update_events::UpdateEvent {
    fn from(event: UpdateEvent) -> Self {
        Self { age: event.timestamp.elapsed(), kind: event.kind }
    }
}

#[derive(Clone, Debug, Default)]
struct UpdateLog {
    current: Option<UpdateState>,
    events: Vec<UpdateEvent>,
}

impl From<UpdateLog> for crate::update_events::UpdateLog {
    fn from(log: UpdateLog) -> Self {
        Self {
            current: log.current.map(Into::into),
            events: log.events.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct UpdatePlanner {
    mgs_client: gateway_client::Client,
    artifact_store: WicketdArtifactStore,
    update_plan: StdMutex<Result<UpdatePlan, UpdatePlanError>>,
    running_updates: Mutex<BTreeMap<SpIdentifier, JoinHandle<()>>>,
    // Note: Our inner mutex here is a standard mutex, not a tokio mutex. We
    // generally hold it only log enough to update its state or push a new
    // update event into its running log; occasionally we hold it long enough to
    // clone it.
    update_logs: Mutex<BTreeMap<SpIdentifier, Arc<StdMutex<UpdateLog>>>>,
    log: Logger,
}

impl UpdatePlanner {
    pub(crate) fn new(
        artifact_store: WicketdArtifactStore,
        mgs_addr: SocketAddrV6,
        log: &Logger,
    ) -> Self {
        let log = log.new(o!("component" => "wicketd update planner"));
        let running_updates = Mutex::default();
        let update_logs = Mutex::default();
        let mgs_client = make_mgs_client(log.clone(), mgs_addr);

        // We don't expect to be able to create an update plan before a TUF
        // repository has been uploaded, and presumably we're being created
        // before such a thing could happen. Therefore, we store the `Result` of
        // creating the update plan; if someone tries to call `start()` before
        // we've been able to successfully generate a plan, we have an error
        // ready to hand them.
        let update_plan = StdMutex::new(UpdatePlan::new(&artifact_store));

        Self {
            mgs_client,
            artifact_store,
            update_plan,
            log,
            running_updates,
            update_logs,
        }
    }

    pub(crate) fn regenerate_plan(&self) -> Result<(), UpdatePlanError> {
        match UpdatePlan::new(&self.artifact_store) {
            Ok(plan) => {
                *self.update_plan.lock().unwrap() = Ok(plan);
                Ok(())
            }
            Err(err) => {
                *self.update_plan.lock().unwrap() = Err(err.clone());
                Err(err)
            }
        }
    }

    pub(crate) async fn start(
        &self,
        sp: SpIdentifier,
    ) -> Result<(), UpdatePlanError> {
        let plan = self.update_plan.lock().unwrap().clone()?;

        let spawn_update_driver = || async {
            // Get a reference to the update log for this SP...
            let update_log = Arc::clone(
                self.update_logs.lock().await.entry(sp).or_default(),
            );

            // ...and then reset it to the empty state, removing any events or
            // state from a previous update attempt.
            *update_log.lock().unwrap() = UpdateLog::default();

            let update_driver = UpdateDriver {
                sp,
                mgs_client: self.mgs_client.clone(),
                update_log,
                log: self.log.new(o!("sp" => format!("{sp:?}"))),
            };
            tokio::spawn(update_driver.run(plan))
        };

        let mut running_updates = self.running_updates.lock().await;
        match running_updates.entry(sp) {
            // Vacant: this is the first time we've started an update to this
            // sp.
            Entry::Vacant(slot) => {
                slot.insert(spawn_update_driver().await);
                Ok(())
            }
            // Occupied: we've previously started an update to this sp; only
            // allow this one if that update is no longer running.
            Entry::Occupied(mut slot) => {
                if slot.get().is_finished() {
                    slot.insert(spawn_update_driver().await);
                    Ok(())
                } else {
                    Err(UpdatePlanError::UpdateInProgress(sp))
                }
            }
        }
    }

    pub(crate) async fn update_log(
        &self,
        sp: SpIdentifier,
    ) -> crate::update_events::UpdateLog {
        let mut update_logs = self.update_logs.lock().await;
        match update_logs.entry(sp) {
            Entry::Vacant(_) => crate::update_events::UpdateLog::default(),
            Entry::Occupied(slot) => slot.get().lock().unwrap().clone().into(),
        }
    }

    /// Clone the current state of the update log for every SP, returning a map
    /// suitable for conversion to JSON.
    pub(crate) async fn update_log_all(
        &self,
    ) -> BTreeMap<SpType, BTreeMap<u32, crate::update_events::UpdateLog>> {
        let update_logs = self.update_logs.lock().await;
        let mut converted_logs = BTreeMap::new();
        for (sp, update_log) in &*update_logs {
            let update_log = update_log.lock().unwrap().clone();
            let inner: &mut BTreeMap<_, _> =
                converted_logs.entry(sp.type_).or_default();
            inner.insert(sp.slot, update_log.into());
        }
        converted_logs
    }
}

#[derive(Debug, Clone, Error)]
pub(crate) enum UpdatePlanError {
    #[error(
        "invalid TUF repository: more than one artifact present of kind {0:?}"
    )]
    DuplicateArtifacts(KnownArtifactKind),
    #[error("invalid TUF repository: no artifact present of kind {0:?}")]
    MissingArtifact(KnownArtifactKind),
    #[error("target is already being updated: {0:?}")]
    UpdateInProgress(SpIdentifier),
}

#[derive(Debug, Clone)]
struct ArtifactIdData {
    id: ArtifactId,
    data: DebugIgnore<BufList>,
}

#[derive(Debug, Clone)]
struct UpdatePlan {
    gimlet_sp: ArtifactIdData,
    psc_sp: ArtifactIdData,
    sidecar_sp: ArtifactIdData,
}

impl UpdatePlan {
    fn new(
        artifact_store: &WicketdArtifactStore,
    ) -> Result<Self, UpdatePlanError> {
        let snapshot = artifact_store.snapshot();

        // We expect exactly one of each of these kinds to be present in the
        // snapshot. Scan the snapshot and record the first of each we find,
        // failing if we find a second.
        let mut gimlet_sp = None;
        let mut psc_sp = None;
        let mut sidecar_sp = None;

        let artifact_found = |out: &mut Option<ArtifactIdData>, id, data| {
            let data = DebugIgnore(data);
            match out.replace(ArtifactIdData { id, data }) {
                None => Ok(()),
                Some(prev) => {
                    // This closure is only called with well-known kinds.
                    let kind = prev.id.kind.to_known().unwrap();
                    Err(UpdatePlanError::DuplicateArtifacts(kind))
                }
            }
        };

        for (id, data) in snapshot {
            let Some(kind) = id.kind.to_known() else { continue };
            match kind {
                KnownArtifactKind::GimletSp => {
                    artifact_found(&mut gimlet_sp, id, data)?
                }
                KnownArtifactKind::PscSp => {
                    artifact_found(&mut psc_sp, id, data)?
                }
                KnownArtifactKind::SwitchSp => {
                    artifact_found(&mut sidecar_sp, id, data)?
                }
                _ => {
                    // ignore other known kinds for now
                }
            }
        }

        Ok(Self {
            gimlet_sp: gimlet_sp.ok_or(UpdatePlanError::MissingArtifact(
                KnownArtifactKind::GimletSp,
            ))?,
            psc_sp: psc_sp.ok_or(UpdatePlanError::MissingArtifact(
                KnownArtifactKind::PscSp,
            ))?,
            sidecar_sp: sidecar_sp.ok_or(UpdatePlanError::MissingArtifact(
                KnownArtifactKind::SwitchSp,
            ))?,
        })
    }
}

#[derive(Debug)]
struct UpdateDriver {
    sp: SpIdentifier,
    mgs_client: gateway_client::Client,
    update_log: Arc<StdMutex<UpdateLog>>,
    log: Logger,
}

impl UpdateDriver {
    async fn run(self, plan: UpdatePlan) {
        if let Err(err) = self.run_impl(plan).await {
            error!(self.log, "update failed"; "err" => ?err);
            self.push_update_failure(err);
        }
    }

    fn set_current_update_state(&self, kind: UpdateStateKind) {
        let state = UpdateState { timestamp: Instant::now(), kind };
        self.update_log.lock().unwrap().current = Some(state);
    }

    fn push_update_success(
        &self,
        kind: UpdateEventSuccessKind,
        new_current: Option<UpdateStateKind>,
    ) {
        let timestamp = Instant::now();
        let kind = UpdateEventKind::Success(kind);
        let event = UpdateEvent { timestamp, kind };
        let mut update_log = self.update_log.lock().unwrap();
        update_log.events.push(event);
        update_log.current =
            new_current.map(|kind| UpdateState { timestamp, kind });
    }

    fn push_update_failure(&self, kind: UpdateEventFailureKind) {
        let kind = UpdateEventKind::Failure(kind);
        let event = UpdateEvent { timestamp: Instant::now(), kind };
        let mut update_log = self.update_log.lock().unwrap();
        update_log.events.push(event);
        update_log.current = None;
    }

    async fn run_impl(
        &self,
        plan: UpdatePlan,
    ) -> Result<(), UpdateEventFailureKind> {
        let sp_artifact = match self.sp.type_ {
            SpType::Sled => &plan.gimlet_sp,
            SpType::Power => &plan.psc_sp,
            SpType::Switch => &plan.sidecar_sp,
        };

        info!(self.log, "starting SP update"; "artifact" => ?sp_artifact.id);
        self.update_sp(sp_artifact).await.map_err(|err| {
            UpdateEventFailureKind::ArtifactUpdateFailed {
                artifact: sp_artifact.id.clone(),
                reason: format!("{err:#}"),
            }
        })?;
        self.push_update_success(
            UpdateEventSuccessKind::ArtifactUpdateComplete {
                artifact: sp_artifact.id.clone(),
            },
            Some(UpdateStateKind::ResettingSp),
        );

        info!(self.log, "all updates complete; resetting SP");
        self.reset_sp().await.map_err(|err| {
            UpdateEventFailureKind::SpResetFailed { reason: format!("{err:#}") }
        })?;
        self.push_update_success(UpdateEventSuccessKind::SpResetComplete, None);

        Ok(())
    }

    async fn update_sp(&self, artifact: &ArtifactIdData) -> anyhow::Result<()> {
        const SP_COMPONENT: &str = SpComponent::SP_ITSELF.const_as_str();

        let image = buf_list_to_vec(&artifact.data);
        let update_id = Uuid::new_v4();
        let body = UpdateBody { id: update_id, image, slot: 0 };
        self.set_current_update_state(UpdateStateKind::SendingArtifactToMgs {
            artifact: artifact.id.clone(),
        });
        self.mgs_client
            .sp_component_update(
                self.sp.type_,
                self.sp.slot,
                SP_COMPONENT,
                &body,
            )
            .await
            .context("failed to start update")?;

        info!(self.log, "waiting for SP update to complete");
        self.set_current_update_state(UpdateStateKind::WaitingForStatus {
            artifact: artifact.id.clone(),
        });
        self.poll_for_component_update_completion(
            &artifact.id,
            update_id,
            SP_COMPONENT,
        )
        .await?;

        Ok(())
    }

    async fn reset_sp(&self) -> anyhow::Result<()> {
        self.mgs_client
            .sp_reset(self.sp.type_, self.sp.slot)
            .await
            .context("failed to reset SP")
            .map(|res| res.into_inner())
    }

    async fn poll_for_component_update_completion(
        &self,
        artifact: &ArtifactId,
        update_id: Uuid,
        component: &str,
    ) -> anyhow::Result<()> {
        // How often we poll MGS for the progress of an update once it starts.
        const STATUS_POLL_FREQ: Duration = Duration::from_millis(300);

        loop {
            let status = self
                .mgs_client
                .sp_component_update_status(
                    self.sp.type_,
                    self.sp.slot,
                    component,
                )
                .await?
                .into_inner();

            match status {
                SpUpdateStatus::None => {
                    bail!("SP no longer processing update (did it reset?")
                }
                SpUpdateStatus::Preparing { id, progress } => {
                    ensure!(id == update_id, "SP processing different update");
                    self.set_current_update_state(
                        UpdateStateKind::PreparingForArtifact {
                            artifact: artifact.clone(),
                            progress,
                        },
                    );
                }
                SpUpdateStatus::InProgress {
                    bytes_received,
                    id,
                    total_bytes,
                } => {
                    ensure!(id == update_id, "SP processing different update");
                    self.set_current_update_state(
                        UpdateStateKind::ArtifactUpdateProgress {
                            bytes_received: bytes_received.into(),
                            total_bytes: total_bytes.into(),
                        },
                    );
                }
                SpUpdateStatus::Complete { id } => {
                    ensure!(id == update_id, "SP processing different update");
                    return Ok(());
                }
                SpUpdateStatus::Aborted { id } => {
                    ensure!(id == update_id, "SP processing different update");
                    bail!("update aborted");
                }
                SpUpdateStatus::Failed { code, id } => {
                    ensure!(id == update_id, "SP processing different update");
                    bail!("update failed (error code {code})");
                }
            }

            tokio::time::sleep(STATUS_POLL_FREQ).await;
        }
    }
}

// Helper function to convert a `BufList` into a `Vec<u8>` for use with
// `gateway_client::Client`.
//
// TODO-performance Can we pass `BufList`s directly (or wrapped) to
// `gateway_client::Client` endpoints without copying the data?
fn buf_list_to_vec(data: &BufList) -> Vec<u8> {
    let mut image = Vec::with_capacity(data.num_bytes());
    for chunk in data {
        image.extend_from_slice(&*chunk);
    }
    image
}
