// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use crate::artifacts::WicketdArtifactStore;
use crate::update_events::UpdateEventFailureKind;
use crate::update_events::UpdateEventKind;
use crate::update_events::UpdateEventSuccessKind;
use crate::update_events::UpdateStateKind;
use anyhow::bail;
use anyhow::ensure;
use anyhow::Context;
use gateway_client::types::SpIdentifier;
use gateway_client::types::SpType;
use gateway_client::types::SpUpdateStatus;
use gateway_client::types::UpdateBody;
use gateway_messages::SpComponent;
use omicron_common::api::internal::nexus::UpdateArtifactId;
use omicron_common::api::internal::nexus::UpdateArtifactKind;
use slog::error;
use slog::info;
use slog::o;
use slog::Logger;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
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
        mgs_client: gateway_client::Client,
        artifact_store: WicketdArtifactStore,
        log: &Logger,
    ) -> Self {
        let log = log.new(o!("component" => "wicketd update planner"));
        let running_updates = Mutex::default();
        let update_logs = Mutex::default();
        Self { mgs_client, artifact_store, log, running_updates, update_logs }
    }

    pub(crate) async fn start(
        &self,
        sp: SpIdentifier,
    ) -> Result<(), UpdatePlanError> {
        let plan = UpdatePlan::new(sp.type_, &self.artifact_store)?;

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
                artifact_store: self.artifact_store.clone(),
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

#[derive(Debug, Error)]
pub(crate) enum UpdatePlanError {
    #[error(
        "invalid TUF repository: more than one artifact present of kind {0:?}"
    )]
    DuplicateArtifacts(UpdateArtifactKind),
    #[error("invalid TUF repository: no artifact present of kind {0:?}")]
    MissingArtifact(UpdateArtifactKind),
    #[error("target is already being updated: {0:?}")]
    UpdateInProgress(SpIdentifier),
}

#[derive(Debug)]
struct UpdatePlan {
    sp: UpdateArtifactId,
}

impl UpdatePlan {
    fn new(
        sp_type: SpType,
        artifact_store: &WicketdArtifactStore,
    ) -> Result<Self, UpdatePlanError> {
        // Given the list of available artifacts, ensure we have exactly 1
        // choice for each kind we need.
        let artifacts = artifact_store.artifact_ids();

        let sp_kind = match sp_type {
            SpType::Sled => UpdateArtifactKind::GimletSp,
            SpType::Power => UpdateArtifactKind::PscSp,
            SpType::Switch => UpdateArtifactKind::SwitchSp,
        };

        let sp = Self::find_exactly_one(sp_kind, &artifacts)?;

        Ok(Self { sp })
    }

    fn find_exactly_one(
        kind: UpdateArtifactKind,
        artifacts: &[UpdateArtifactId],
    ) -> Result<UpdateArtifactId, UpdatePlanError> {
        let mut found = None;
        for artifact in artifacts.iter().filter(|id| id.kind == kind) {
            if found.replace(artifact.clone()).is_some() {
                return Err(UpdatePlanError::DuplicateArtifacts(kind));
            }
        }

        found.ok_or(UpdatePlanError::MissingArtifact(kind))
    }
}

#[derive(Debug)]
struct UpdateDriver {
    sp: SpIdentifier,
    mgs_client: gateway_client::Client,
    artifact_store: WicketdArtifactStore,
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
        info!(self.log, "starting SP update"; "artifact" => ?plan.sp);
        self.update_sp(plan.sp.clone()).await.map_err(|err| {
            UpdateEventFailureKind::ArtifactUpdateFailed {
                artifact: plan.sp.clone(),
                reason: format!("{err:#}"),
            }
        })?;
        self.push_update_success(
            UpdateEventSuccessKind::ArtifactUpdateComplete {
                artifact: plan.sp,
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

    async fn update_sp(
        &self,
        artifact: UpdateArtifactId,
    ) -> anyhow::Result<()> {
        const SP_COMPONENT: &str = SpComponent::SP_ITSELF.const_as_str();

        let image = self.artifact_data(&artifact)?;
        let update_id = Uuid::new_v4();
        let body = UpdateBody { id: update_id, image, slot: 0 };
        self.set_current_update_state(UpdateStateKind::SendingArtifactToMgs {
            artifact: artifact.clone(),
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
            artifact: artifact.clone(),
        });
        self.poll_for_component_update_completion(
            artifact,
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

    fn artifact_data(&self, id: &UpdateArtifactId) -> anyhow::Result<Vec<u8>> {
        let data = self.artifact_store.get(id).with_context(|| {
            format!("missing artifact {id:?}: did the TUF repository change?")
        })?;

        // TODO Convert a BufList into a Vec<u8> - this is a little gross.
        // Should MGS's endpoint accept a BufList somehow? Should the artifact
        // store give us something more amenable to conversion?
        let mut image = Vec::with_capacity(data.num_bytes());
        for chunk in data {
            image.extend_from_slice(&*chunk);
        }

        Ok(image)
    }

    async fn poll_for_component_update_completion(
        &self,
        artifact: UpdateArtifactId,
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
