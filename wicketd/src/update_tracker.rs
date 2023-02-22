// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use crate::artifacts::ArtifactIdData;
use crate::artifacts::UpdatePlan;
use crate::mgs::make_mgs_client;
use crate::update_events::UpdateEventFailureKind;
use crate::update_events::UpdateEventKind;
use crate::update_events::UpdateEventSuccessKind;
use crate::update_events::UpdateStateKind;
use anyhow::bail;
use anyhow::ensure;
use anyhow::Context;
use buf_list::BufList;
use bytes::Bytes;
use futures::TryStream;
use gateway_client::types::SpIdentifier;
use gateway_client::types::SpType;
use gateway_client::types::SpUpdateStatus;
use gateway_messages::SpComponent;
use omicron_common::backoff;
use omicron_common::update::ArtifactId;
use slog::error;
use slog::info;
use slog::o;
use slog::warn;
use slog::Logger;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::net::SocketAddrV6;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::Duration;
use std::time::Instant;
use thiserror::Error;
use tokio::sync::watch;
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
struct SpUpdateData {
    task: JoinHandle<()>,
    // Note: Our mutex here is a standard mutex, not a tokio mutex. We generally
    // hold it only log enough to update its state or push a new update event
    // into its running log; occasionally we hold it long enough to clone it.
    update_log: Arc<StdMutex<UpdateLog>>,
}

#[derive(Debug)]
struct UploadTrampolinePhase2ToMgsStatus {
    id: ArtifactId,
    // The upload task retries forever until it succeeds, so we don't need to
    // keep a "tried but failed" variant here; we just need to know if the
    // upload has completed.
    complete: bool,
}

#[derive(Debug)]
struct UploadTrampolinePhase2ToMgs {
    // The tuple is the ID of the Trampoline image and a boolean for whether or
    // not it is complete. The upload task retries forever until it succeeds, so
    // we don't need to keep a "tried but failed" variant here.
    status: watch::Receiver<UploadTrampolinePhase2ToMgsStatus>,
    task: JoinHandle<()>,
}

#[derive(Debug)]
pub(crate) struct UpdateTracker {
    mgs_client: gateway_client::Client,
    sp_update_data: Mutex<BTreeMap<SpIdentifier, SpUpdateData>>,

    // Every sled update via trampoline requires MGS to serve the trampoline
    // phase 2 image to the sled's SP over the management network; however, that
    // doesn't mean we should upload the trampoline image to MGS for every sled
    // update - it's always the same (for any given update plan). Therefore, we
    // separate the status of uploading the trampoline phase 2 MGS from the
    // status of individual SP updates: we'll start this upload the first time a
    // sled update starts that uses it, and any update (including that one or
    // any future sled updates) will pause at the appropriate time (if needed)
    // to wait for the upload to complete.
    upload_trampoline_phase_2_to_mgs:
        Mutex<Option<UploadTrampolinePhase2ToMgs>>,

    log: Logger,
}

impl UpdateTracker {
    pub(crate) fn new(mgs_addr: SocketAddrV6, log: &Logger) -> Self {
        let log = log.new(o!("component" => "wicketd update planner"));
        let sp_update_data = Mutex::default();
        let mgs_client = make_mgs_client(log.clone(), mgs_addr);
        let upload_trampoline_phase_2_to_mgs = Mutex::default();

        Self {
            mgs_client,
            sp_update_data,
            log,
            upload_trampoline_phase_2_to_mgs,
        }
    }

    pub(crate) async fn start(
        &self,
        sp: SpIdentifier,
        plan: UpdatePlan,
    ) -> Result<(), StartUpdateError> {
        // Do we need to upload this plan's trampoline phase 2 to MGS?
        let upload_trampoline_phase_2_to_mgs = {
            let mut upload_trampoline_phase_2_to_mgs =
                self.upload_trampoline_phase_2_to_mgs.lock().await;

            match upload_trampoline_phase_2_to_mgs.as_mut() {
                Some(prev) => {
                    // We've previously started an upload - does it match this
                    // update's artifact ID? If not, cancel the old task (which
                    // might still be trying to upload) and start a new one with
                    // our current image.
                    //
                    // TODO-correctness If we still have updates running that
                    // expect the old image, they're probably going to fail.
                    // Should we handle that more cleanly or just let them fail?
                    if prev.status.borrow().id != plan.trampoline_phase_2.id {
                        // It does _not_ match - we have a new plan with a
                        // different trampoline image. If the old task is still
                        // running, cancel it, and start a new one.
                        prev.task.abort();
                        *prev =
                            self.spawn_upload_trampoline_phase_2_to_mgs(&plan);
                    }
                }
                None => {
                    *upload_trampoline_phase_2_to_mgs = Some(
                        self.spawn_upload_trampoline_phase_2_to_mgs(&plan),
                    );
                }
            }

            // Both branches above leave `upload_trampoline_phase_2_to_mgs` with
            // data, so we can unwrap here to clone the `watch` channel.
            upload_trampoline_phase_2_to_mgs.as_ref().unwrap().status.clone()
        };

        let spawn_update_driver = || async {
            let update_log = Arc::default();

            let update_driver = UpdateDriver {
                sp,
                mgs_client: self.mgs_client.clone(),
                upload_trampoline_phase_2_to_mgs,
                update_log: Arc::clone(&update_log),
                log: self.log.new(o!("sp" => format!("{sp:?}"))),
            };

            let task = tokio::spawn(update_driver.run(plan));

            SpUpdateData { task, update_log }
        };

        let mut sp_update_data = self.sp_update_data.lock().await;
        match sp_update_data.entry(sp) {
            // Vacant: this is the first time we've started an update to this
            // sp.
            Entry::Vacant(slot) => {
                slot.insert(spawn_update_driver().await);
                Ok(())
            }
            // Occupied: we've previously started an update to this sp; only
            // allow this one if that update is no longer running.
            Entry::Occupied(mut slot) => {
                if slot.get().task.is_finished() {
                    slot.insert(spawn_update_driver().await);
                    Ok(())
                } else {
                    Err(StartUpdateError::UpdateInProgress(sp))
                }
            }
        }
    }

    fn spawn_upload_trampoline_phase_2_to_mgs(
        &self,
        plan: &UpdatePlan,
    ) -> UploadTrampolinePhase2ToMgs {
        let artifact = plan.trampoline_phase_2.clone();
        let (status_tx, status_rx) =
            watch::channel(UploadTrampolinePhase2ToMgsStatus {
                id: artifact.id.clone(),
                complete: false,
            });
        let task = tokio::spawn(upload_trampoline_phase_2_to_mgs(
            self.mgs_client.clone(),
            artifact,
            status_tx,
            self.log.clone(),
        ));
        UploadTrampolinePhase2ToMgs { status: status_rx, task }
    }

    pub(crate) async fn update_log(
        &self,
        sp: SpIdentifier,
    ) -> crate::update_events::UpdateLog {
        let mut sp_update_data = self.sp_update_data.lock().await;
        match sp_update_data.entry(sp) {
            Entry::Vacant(_) => crate::update_events::UpdateLog::default(),
            Entry::Occupied(slot) => {
                slot.get().update_log.lock().unwrap().clone().into()
            }
        }
    }

    /// Clone the current state of the update log for every SP, returning a map
    /// suitable for conversion to JSON.
    pub(crate) async fn update_log_all(
        &self,
    ) -> BTreeMap<SpType, BTreeMap<u32, crate::update_events::UpdateLog>> {
        let sp_update_data = self.sp_update_data.lock().await;
        let mut converted_logs = BTreeMap::new();
        for (sp, update_data) in &*sp_update_data {
            let update_log = update_data.update_log.lock().unwrap().clone();
            let inner: &mut BTreeMap<_, _> =
                converted_logs.entry(sp.type_).or_default();
            inner.insert(sp.slot, update_log.into());
        }
        converted_logs
    }
}

#[derive(Debug, Clone, Error)]
pub(crate) enum StartUpdateError {
    #[error("target is already being updated: {0:?}")]
    UpdateInProgress(SpIdentifier),
}

#[derive(Debug)]
struct UpdateDriver {
    sp: SpIdentifier,
    mgs_client: gateway_client::Client,
    upload_trampoline_phase_2_to_mgs:
        watch::Receiver<UploadTrampolinePhase2ToMgsStatus>,
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

        let update_id = Uuid::new_v4();

        // The SP only has one updateable firmware slot ("the inactive bank") -
        // we always pass 0.
        let firmware_slot = 0;

        self.set_current_update_state(UpdateStateKind::SendingArtifactToMgs {
            artifact: artifact.id.clone(),
        });
        self.mgs_client
            .sp_component_update(
                self.sp.type_,
                self.sp.slot,
                SP_COMPONENT,
                firmware_slot,
                &update_id,
                reqwest::Body::wrap_stream(buf_list_to_try_stream(
                    artifact.data.0.clone(),
                )),
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

fn buf_list_to_try_stream(
    data: BufList,
) -> impl TryStream<Ok = Bytes, Error = std::convert::Infallible> {
    futures::stream::iter(data.into_iter().map(Ok))
}

async fn upload_trampoline_phase_2_to_mgs(
    mgs_client: gateway_client::Client,
    artifact: ArtifactIdData,
    status: watch::Sender<UploadTrampolinePhase2ToMgsStatus>,
    log: Logger,
) {
    let data = artifact.data;
    let upload_task = move || {
        let mgs_client = mgs_client.clone();
        let image = buf_list_to_try_stream(data.0.clone());

        async move {
            mgs_client
                .recovery_host_phase2_upload(reqwest::Body::wrap_stream(image))
                .await
                .map_err(|e| backoff::BackoffError::transient(e.to_string()))
        }
    };

    let log_failure = move |err, delay| {
        warn!(
            log,
            "failed to upload trampoline phase 2 to MGS, will retry in {:?}",
            delay;
            "err" => %err,
        );
    };

    // retry_policy_internal_service_aggressive() retries forever, so we can
    // unwrap this call to retry_notify
    backoff::retry_notify(
        backoff::retry_policy_internal_service_aggressive(),
        upload_task,
        log_failure,
    )
    .await
    .unwrap();

    // Notify all receivers that we've uploaded the image.
    _ = status.send(UploadTrampolinePhase2ToMgsStatus {
        id: artifact.id,
        complete: true,
    });

    // Wait for all receivers to be gone before we exit, so they don't get recv
    // errors unless we're cancelled.
    status.closed().await;
}
