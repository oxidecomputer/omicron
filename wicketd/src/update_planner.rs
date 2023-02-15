// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use crate::artifacts::Artifact;
use crate::artifacts::ArtifactSnapshot;
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
use gateway_client::types::HostStartupOptions;
use gateway_client::types::InstallinatorImageId;
use gateway_client::types::PowerState;
use gateway_client::types::SpIdentifier;
use gateway_client::types::SpType;
use gateway_client::types::SpUpdateStatus;
use gateway_client::types::UpdateBody;
use gateway_messages::SpComponent;
use omicron_common::api::internal::nexus::UpdateArtifactId;
use omicron_common::backoff;
use sha3::Digest;
use sha3::Sha3_256;
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
pub(crate) struct UpdatePlanner {
    mgs_client: gateway_client::Client,
    artifact_store: WicketdArtifactStore,
    running_updates: Mutex<BTreeMap<SpIdentifier, JoinHandle<()>>>,
    snapshot_state: Arc<StdMutex<Option<SnapshotState>>>,
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
        Self {
            mgs_client: make_mgs_client(log.clone(), mgs_addr),
            artifact_store,
            running_updates: Mutex::default(),
            snapshot_state: Arc::default(),
            update_logs: Mutex::default(),
            log,
        }
    }

    pub(crate) async fn take_artifact_snapshot(&self) -> anyhow::Result<()> {
        let snapshot = match self.artifact_store.get_snapshot() {
            Ok(snapshot) => snapshot,
            Err(err) => {
                // Our artifact store is now invalid; if we had a snapshot,
                // clear it out, so we don't try to start new updates moving
                // forward.
                *self.snapshot_state.lock().unwrap() = None;
                return Err(err);
            }
        };

        let (upload_done_tx, upload_done_rx) = watch::channel(false);

        // Immediately spawn a task to upload the trampoline phase 2 to MGS
        // under the assumption that this snapshot will be used for recovery.
        tokio::spawn(upload_trampoline_phase_2_to_mgs(
            self.mgs_client.clone(),
            snapshot.trampoline_phase_2.clone(),
            upload_done_tx,
            self.log.clone(),
        ));

        let snapshot_state = SnapshotState {
            snapshot,
            uploaded_trampoline_phase_2_to_mgs: upload_done_rx,
        };

        *self.snapshot_state.lock().unwrap() = Some(snapshot_state);

        Ok(())
    }

    pub(crate) async fn start(
        &self,
        sp: SpIdentifier,
    ) -> Result<(), UpdatePlanError> {
        let snapshot_state = self
            .snapshot_state
            .lock()
            .unwrap()
            .clone()
            .ok_or(UpdatePlanError::NoValidRepository)?;

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
            tokio::spawn(update_driver.run(
                snapshot_state.snapshot,
                snapshot_state.uploaded_trampoline_phase_2_to_mgs,
            ))
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
    #[error("no valid TUF repository has been uploaded")]
    NoValidRepository,
    #[error("target is already being updated: {0:?}")]
    UpdateInProgress(SpIdentifier),
}

#[derive(Debug)]
struct UpdateDriver {
    sp: SpIdentifier,
    mgs_client: gateway_client::Client,
    update_log: Arc<StdMutex<UpdateLog>>,
    log: Logger,
}

impl UpdateDriver {
    async fn run(
        self,
        snapshot: ArtifactSnapshot,
        mut uploaded_trampoline_phase_2_to_mgs: watch::Receiver<bool>,
    ) {
        if let Err(err) = self
            .run_impl(&snapshot, &mut uploaded_trampoline_phase_2_to_mgs)
            .await
        {
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
        snapshot: &ArtifactSnapshot,
        uploaded_trampoline_phase_2_to_mgs: &mut watch::Receiver<bool>,
    ) -> Result<(), UpdateEventFailureKind> {
        let sp_artifact = match self.sp.type_ {
            SpType::Sled => &snapshot.gimlet_sp,
            SpType::Power => todo!(),
            SpType::Switch => todo!(),
        };

        // TODO-correctness Confirm the order of updates here - do we update and
        // reset the SP before updating the host?
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

        info!(self.log, "SP update delivery complete; resetting SP");
        self.reset_sp().await.map_err(|err| {
            UpdateEventFailureKind::SpResetFailed { reason: format!("{err:#}") }
        })?;

        if self.sp.type_ == SpType::Sled {
            self.run_sled(snapshot, uploaded_trampoline_phase_2_to_mgs).await?;
        }

        self.push_update_success(UpdateEventSuccessKind::Done, None);
        Ok(())
    }

    async fn update_sp(&self, artifact: &Artifact) -> anyhow::Result<()> {
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

    async fn run_sled(
        &self,
        snapshot: &ArtifactSnapshot,
        uploaded_trampoline_phase_2_to_mgs: &mut watch::Receiver<bool>,
    ) -> Result<(), UpdateEventFailureKind> {
        info!(self.log, "starting host recovery");

        // We arbitrarily choose to store the trampoline phase 1 in host boot
        // slot 0.
        let trampoline_phase_1_boot_slot = 0;
        self.deliver_host_phase1(
            &snapshot.trampoline_phase_1,
            trampoline_phase_1_boot_slot,
        )
        .await
        .map_err(|err| {
            UpdateEventFailureKind::ArtifactUpdateFailed {
                artifact: snapshot.trampoline_phase_1.id.clone(),
                reason: format!("{err:#}"),
            }
        })?;

        // Ensure we've finished sending the trampoline phase 2 to MGS, or wait
        // until we have.
        self.set_current_update_state(UpdateStateKind::SendingArtifactToMgs {
            artifact: snapshot.trampoline_phase_2.id.clone(),
        });
        while !*uploaded_trampoline_phase_2_to_mgs.borrow() {
            info!(
                self.log,
                "waiting for trampoline phase 2 upload to MGS to complete"
            );

            // `upload_trampoline_phase_2_to_mgs()` waits for all receivers to
            // be dropped, so the only way `changed()` can fail is if that task
            // has panicked; unwrap to propogate such panics.
            uploaded_trampoline_phase_2_to_mgs.changed().await.unwrap();
        }

        info!(self.log, "starting installinator portion of host recovery");
        self.drive_installinator(snapshot).await.map_err(|err| {
            UpdateEventFailureKind::InstallinatorFailed {
                reason: format!("{err:#}"),
            }
        })?;

        // TODO-correctness Which M.2 slot does installinator use? This assumes
        // it writes to the 0 slot, so we overwrite the trampoline phase 1 with
        // the real phase 1, now that installinator is finished.
        self.deliver_host_phase1(
            &snapshot.host_phase_1,
            trampoline_phase_1_boot_slot,
        )
        .await
        .map_err(|err| {
            UpdateEventFailureKind::ArtifactUpdateFailed {
                artifact: snapshot.host_phase_1.id.clone(),
                reason: format!("{err:#}"),
            }
        })?;

        // Recovery complete! Boot the host.
        self.set_host_power_state(PowerState::A0).await.map_err(|err| {
            UpdateEventFailureKind::ArtifactUpdateFailed {
                artifact: snapshot.host_phase_1.id.clone(),
                reason: format!("{err:#}"),
            }
        })?;

        Ok(())
    }

    async fn deliver_host_phase1(
        &self,
        artifact: &Artifact,
        host_flash_slot: u16,
    ) -> anyhow::Result<()> {
        const HOST_BOOT_FLASH: &str =
            SpComponent::HOST_CPU_BOOT_FLASH.const_as_str();

        let phase1_image = buf_list_to_vec(&artifact.data);

        // Ensure host is in A2.
        self.set_host_power_state(PowerState::A2).await?;

        // Start delivering image.
        info!(self.log, "sending trampoline phase 1");
        let update_id = Uuid::new_v4();
        self.set_current_update_state(UpdateStateKind::SendingArtifactToMgs {
            artifact: artifact.id.clone(),
        });
        self.mgs_client
            .sp_component_update(
                self.sp.type_,
                self.sp.slot,
                HOST_BOOT_FLASH,
                &UpdateBody {
                    id: update_id,
                    image: phase1_image,
                    slot: host_flash_slot,
                },
            )
            .await
            .context("failed to write host boot flash slot A")?;

        // Wait for image delivery to complete.
        info!(self.log, "waiting for trampoline phase 1 deliver to complete");
        self.set_current_update_state(UpdateStateKind::WaitingForStatus {
            artifact: artifact.id.clone(),
        });
        self.poll_for_component_update_completion(
            &artifact.id,
            update_id,
            HOST_BOOT_FLASH,
        )
        .await
    }

    async fn drive_installinator(
        &self,
        snapshot: &ArtifactSnapshot,
    ) -> anyhow::Result<()> {
        // Set the installinator image ID, so installinator knows the hashes of
        // the real phase2 and control plane images it needs to fetch.
        let update_id = Uuid::new_v4();

        // TODO-performance Every update recomputes these hashes; host phase 2
        // is large, so maybe we should only do it once?
        let hashes = InstallinatorImageIdHashes::from(snapshot);

        let installinator_image_id = InstallinatorImageId {
            update_id,
            host_phase_2: hashes.host_phase_2_hash.to_vec(),
            control_plane: hashes.control_plane_hash.to_vec(),
        };
        info!(
            self.log, "setting installinator image ID";
            "update_id" => %update_id,
        );
        self.set_current_update_state(
            UpdateStateKind::SettingInstallinatorOptions,
        );
        self.mgs_client
            .sp_installinator_image_id_set(
                self.sp.type_,
                self.sp.slot,
                &installinator_image_id,
            )
            .await
            .context("failed to set installinator image ID")?;

        // Tell the host to boot in phase 2 recovery mode (i.e., fetch the host
        // phase 2 image from MGS over the management network).
        // TODO-completeness We may decide to push this startup option into the
        // phase1 recovery image itself, which would remove the need for this
        // call.
        info!(self.log, "setting host startup option for phase 2 recovery");
        self.set_current_update_state(
            UpdateStateKind::SettingHostStartupOptions,
        );
        self.mgs_client
            .sp_startup_options_set(
                self.sp.type_,
                self.sp.slot,
                &HostStartupOptions {
                    boot_net: false,
                    boot_ramdisk: false,
                    bootrd: false,
                    kbm: false,
                    kmdb: false,
                    kmdb_boot: false,
                    phase2_recovery_mode: true,
                    prom: false,
                    verbose: false,
                },
            )
            .await
            .context("failed to set trampoline recovery startup options")?;

        // Boot the host.
        self.set_host_power_state(PowerState::A0).await?;

        // TODO-completeness We now have to wait for the host to boot, which
        // involves the SP feeding the trampoline phase 2 image to the host over
        // the UART. We could ask MGS for (indirect) progress here, but it's not
        // strictly necessary - we need to wait until installinator finishes, so
        // for now we'll skip trampoline phase 2 progress and move on to that.

        info!(self.log, "waiting for installinator to complete");
        // TODO How do we wait for installinator?

        // Cleanup: Tell the SP to forget this installinator ID.
        info!(self.log, "clearing installinator image ID");
        self.set_current_update_state(
            UpdateStateKind::SettingInstallinatorOptions,
        );
        self.mgs_client
            .sp_installinator_image_id_delete(self.sp.type_, self.sp.slot)
            .await
            .context("failed to clear installinator image ID")?;

        Ok(())
    }

    async fn set_host_power_state(
        &self,
        power_state: PowerState,
    ) -> anyhow::Result<()> {
        info!(self.log, "moving host to {power_state:?}");
        self.set_current_update_state(UpdateStateKind::SettingHostPowerState {
            power_state,
        });
        self.mgs_client
            .sp_power_state_set(self.sp.type_, self.sp.slot, power_state)
            .await
            .with_context(|| format!("failed to put sled into {power_state:?}"))
            .map(|response| response.into_inner())
    }

    async fn poll_for_component_update_completion(
        &self,
        artifact: &UpdateArtifactId,
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

#[derive(Debug, Clone)]
struct SnapshotState {
    snapshot: ArtifactSnapshot,
    uploaded_trampoline_phase_2_to_mgs: watch::Receiver<bool>,
}

// Information we need to extract exactly once from a given TUF repository that
// we use across multiple recovery operations.
#[derive(Debug, Clone, Copy)]
struct InstallinatorImageIdHashes {
    host_phase_2_hash: [u8; 32],
    control_plane_hash: [u8; 32],
}

impl From<&'_ ArtifactSnapshot> for InstallinatorImageIdHashes {
    fn from(snapshot: &'_ ArtifactSnapshot) -> Self {
        let mut digest = Sha3_256::new();
        for chunk in &snapshot.host_phase_2.data {
            digest.update(chunk);
        }
        let host_phase_2_hash = digest.finalize().into();

        let mut digest = Sha3_256::new();
        for chunk in &snapshot.control_plane.data {
            digest.update(chunk);
        }
        let control_plane_hash = digest.finalize().into();

        Self { host_phase_2_hash, control_plane_hash }
    }
}

fn buf_list_to_vec(data: &BufList) -> Vec<u8> {
    let mut image = Vec::with_capacity(data.num_bytes());
    for chunk in data {
        image.extend_from_slice(&*chunk);
    }
    image
}

async fn upload_trampoline_phase_2_to_mgs(
    mgs_client: gateway_client::Client,
    artifact: Artifact,
    done: watch::Sender<bool>,
    log: Logger,
) {
    let data = artifact.data;
    let upload_task = move || {
        let mgs_client = mgs_client.clone();
        let data = data.clone();

        // TODO Convert a BufList into a Vec<u8> - this is a little gross.
        // Should MGS's endpoint accept a BufList somehow?
        let image = buf_list_to_vec(&data);

        async move {
            mgs_client
                .recovery_host_phase2_upload(image)
                .await
                .map_err(|e| backoff::BackoffError::transient(e.to_string()))
        }
    };

    let log_failure = move |err, delay| {
        warn!(
            log,
            "failed to upload trampoline phase 2 to MGS, will retry in {:?}",
            delay;
            "err" => err,
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
    _ = done.send(true);

    // Wait for all receivers to be gone before we exit, so they never get recv
    // errors.
    done.closed().await;
}
