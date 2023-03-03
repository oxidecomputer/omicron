// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use crate::artifacts::ArtifactIdData;
use crate::artifacts::UpdatePlan;
use crate::installinator_progress::IprStartReceiver;
use crate::installinator_progress::IprUpdateTracker;
use crate::mgs::make_mgs_client;
use crate::update_events::UpdateEventFailureKind;
use crate::update_events::UpdateEventKind;
use crate::update_events::UpdateEventSuccessKind;
use crate::update_events::UpdateStateKind;
use anyhow::anyhow;
use anyhow::bail;
use anyhow::ensure;
use anyhow::Context;
use buf_list::BufList;
use bytes::Bytes;
use display_error_chain::DisplayErrorChain;
use dropshot::HttpError;
use futures::TryStream;
use gateway_client::types::HostPhase2Progress;
use gateway_client::types::HostPhase2RecoveryImageId;
use gateway_client::types::HostStartupOptions;
use gateway_client::types::InstallinatorImageId;
use gateway_client::types::PowerState;
use gateway_client::types::SpComponentFirmwareSlot;
use gateway_client::types::SpIdentifier;
use gateway_client::types::SpType;
use gateway_client::types::SpUpdateStatus;
use gateway_messages::SpComponent;
use installinator_common::ProgressEventKind;
use installinator_common::ProgressReport;
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
use tokio::sync::mpsc;
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
    // keep a "tried but failed" variant here; we just need to know the ID of
    // the uploaded image once it's done.
    uploaded_image_id: Option<HostPhase2RecoveryImageId>,
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
    ipr_update_tracker: IprUpdateTracker,
}

impl UpdateTracker {
    pub(crate) fn new(
        mgs_addr: SocketAddrV6,
        log: &Logger,
        ipr_update_tracker: IprUpdateTracker,
    ) -> Self {
        let log = log.new(o!("component" => "wicketd update planner"));
        let sp_update_data = Mutex::default();
        let mgs_client = make_mgs_client(log.clone(), mgs_addr);
        let upload_trampoline_phase_2_to_mgs = Mutex::default();

        Self {
            mgs_client,
            sp_update_data,
            log,
            upload_trampoline_phase_2_to_mgs,
            ipr_update_tracker,
        }
    }

    pub(crate) async fn start(
        &self,
        sp: SpIdentifier,
        plan: UpdatePlan,
        update_id: Uuid,
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
            let ipr_start_receiver =
                self.ipr_update_tracker.register(update_id).await;

            let update_driver = UpdateDriver {
                update_id,
                sp,
                mgs_client: self.mgs_client.clone(),
                upload_trampoline_phase_2_to_mgs,
                update_log: Arc::clone(&update_log),
                log: self.log.new(o!(
                    "sp" => format!("{sp:?}"),
                    "update_id" => update_id.to_string(),
                )),
            };

            let task =
                tokio::spawn(update_driver.run(plan, ipr_start_receiver));

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
                uploaded_image_id: None,
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

impl StartUpdateError {
    pub(crate) fn to_http_error(&self) -> HttpError {
        let message = DisplayErrorChain::new(self).to_string();

        match self {
            StartUpdateError::UpdateInProgress(_) => {
                HttpError::for_bad_request(None, message)
            }
        }
    }
}

#[derive(Debug)]
struct UpdateDriver {
    update_id: Uuid,
    sp: SpIdentifier,
    mgs_client: gateway_client::Client,
    upload_trampoline_phase_2_to_mgs:
        watch::Receiver<UploadTrampolinePhase2ToMgsStatus>,
    update_log: Arc<StdMutex<UpdateLog>>,
    log: Logger,
}

impl UpdateDriver {
    async fn run(
        mut self,
        plan: UpdatePlan,
        ipr_start_receiver: IprStartReceiver,
    ) {
        if let Err(err) = self.run_impl(plan, ipr_start_receiver).await {
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
        &mut self,
        plan: UpdatePlan,
        ipr_start_receiver: IprStartReceiver,
    ) -> Result<(), UpdateEventFailureKind> {
        // TODO-correctness Is the general order here correct? Host then SP then
        // RoT, then reboot the SP/RoT?
        if self.sp.type_ == SpType::Sled {
            self.run_sled(&plan, ipr_start_receiver).await?;
        }

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

    async fn run_sled(
        &mut self,
        plan: &UpdatePlan,
        ipr_start_receiver: IprStartReceiver,
    ) -> Result<(), UpdateEventFailureKind> {
        info!(self.log, "starting host recovery via trampoline image");

        let uploaded_trampoline_phase2_id =
            self.install_trampoline_image(plan).await.map_err(|err| {
                UpdateEventFailureKind::ArtifactUpdateFailed {
                    // `trampoline_phase_1` and `trampoline_phase_2` have the same
                    // artifact ID, so the choice here is arbitrary.
                    artifact: plan.trampoline_phase_1.id.clone(),
                    reason: format!("{err:#}"),
                }
            })?;

        let mut ipr_receiver = self
            .wait_for_first_installinator_progress(
                plan,
                ipr_start_receiver,
                uploaded_trampoline_phase2_id,
            )
            .await
            .map_err(|err| {
                UpdateEventFailureKind::ArtifactUpdateFailed {
                    // `trampoline_phase_1` and `trampoline_phase_2` have the
                    // same artifact ID, so the choice here is arbitrary.
                    artifact: plan.trampoline_phase_1.id.clone(),
                    reason: format!("{err:#}"),
                }
            })?;

        while let Some(report) = ipr_receiver.recv().await {
            self.process_installinator_report(report).await;
        }

        // The receiver being closed means that the installinator has completed.

        // Installinator is done: install the host phase 1 that matches the host
        // phase 2 it installed, and boot our newly-recovered sled.
        self.install_host_phase_1_and_boot(plan).await.map_err(|err| {
            UpdateEventFailureKind::ArtifactUpdateFailed {
                artifact: plan.host_phase_1.id.clone(),
                reason: format!("{err:#}"),
            }
        })?;

        Ok(())
    }

    async fn wait_for_first_installinator_progress(
        &self,
        plan: &UpdatePlan,
        mut ipr_start_receiver: IprStartReceiver,
        uploaded_trampoline_phase2_id: HostPhase2RecoveryImageId,
    ) -> anyhow::Result<mpsc::Receiver<ProgressReport>> {
        const MGS_PROGRESS_POLL_INTERVAL: Duration = Duration::from_secs(3);

        // Waiting for the installinator to start is a little strange. It can't
        // start until the host boots, which requires all the normal boot things
        // (DRAM training, etc.), but also fetching the trampoline phase 2 image
        // over the management network -> SP -> uart path, which runs at about
        // 167 KiB/sec. This is a _long_ time to wait with no visible progress,
        // so we'll query MGS for progress of that phase 2 trampoline delivery.
        // However, this query is "best effort" - MGS is observing progress
        // indirectly (i.e., "what was the last request for a phase 2 image I
        // got from this SP"), and it isn't definitive. We'll still report it as
        // long as it matches the trampoline image we're expecting the SP to be
        // pulling, but it's possible we could be seeing stale status from a
        // previous update attempt with the same image.
        //
        // To start, _clear out_ the most recent status that MGS may have
        // cached, so we don't see any stale progress from a previous update
        // through this SP. If somehow we've lost the race and our SP is already
        // actively requesting host blocks, this will discard a real progress
        // message, but that's fine - in that case we expect to see another real
        // one imminently. It's possible (but hopefully unlikely?) that the SP
        // is getting its phase two image from the _other_ scrimlet's MGS
        // instance, in which case we will get no progress info at all until
        // installinator starts reporting in.
        //
        // Throughout this function, we do not fail if a request to MGS fails -
        // these are all "best effort" progress; our real failure mode is if
        // installinator tells us it has failed.
        if let Err(err) = self
            .mgs_client
            .sp_host_phase2_progress_delete(self.sp.type_, self.sp.slot)
            .await
        {
            warn!(
                self.log, "failed to clear SP host phase2 progress";
                "err" => %err,
            );
        }

        self.set_current_update_state(
            UpdateStateKind::WaitingForTrampolineImageDelivery {
                artifact: plan.trampoline_phase_1.id.clone(),
                progress: HostPhase2Progress::None,
            },
        );

        let mut interval = tokio::time::interval(MGS_PROGRESS_POLL_INTERVAL);
        interval
            .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                receiver = &mut ipr_start_receiver => {
                    // Received the first progress from the installinator.
                    break receiver.context("start sender died");
                }
                _ = interval.tick() => {
                    self.poll_host_phase2_progress(plan, &uploaded_trampoline_phase2_id).await;
                }
            }
        }
    }

    async fn poll_host_phase2_progress(
        &self,
        plan: &UpdatePlan,
        uploaded_trampoline_phase2_id: &HostPhase2RecoveryImageId,
    ) {
        match self
            .mgs_client
            .sp_host_phase2_progress_get(self.sp.type_, self.sp.slot)
            .await
            .map(|response| response.into_inner())
        {
            Ok(HostPhase2Progress::Available {
                age,
                image_id,
                offset,
                total_size,
            }) => {
                // Does this image ID match the one we uploaded? If so,
                // record our current progress; if not, this is probably
                // stale data from a past update, and we have no progress
                // information.
                let progress = if &image_id == uploaded_trampoline_phase2_id {
                    HostPhase2Progress::Available {
                        age,
                        image_id,
                        offset,
                        total_size,
                    }
                } else {
                    HostPhase2Progress::None
                };
                self.set_current_update_state(
                    UpdateStateKind::WaitingForTrampolineImageDelivery {
                        artifact: plan.trampoline_phase_1.id.clone(),
                        progress,
                    },
                );
            }
            Ok(HostPhase2Progress::None) => {
                self.set_current_update_state(
                    UpdateStateKind::WaitingForTrampolineImageDelivery {
                        artifact: plan.trampoline_phase_1.id.clone(),
                        progress: HostPhase2Progress::None,
                    },
                );
            }
            Err(err) => {
                warn!(
                    self.log, "failed to get SP host phase2 progress";
                    "err" => %err,
                );
            }
        }
    }

    async fn process_installinator_report(
        &mut self,
        mut report: ProgressReport,
    ) {
        // Currently, progress reports have zero or one progress events. Don't
        // assert that here, in case this version of wicketd is updating a
        // future installinator which reports multiple progress events.
        let kind = if let Some(event) = report.progress_events.drain(..).next()
        {
            match event.kind {
                ProgressEventKind::DownloadProgress {
                    attempt,
                    kind,
                    peer: _peer,
                    downloaded_bytes,
                    total_bytes,
                    elapsed,
                } => UpdateStateKind::ArtifactDownloadProgress {
                    attempt,
                    kind,
                    downloaded_bytes,
                    total_bytes,
                    elapsed,
                },
                ProgressEventKind::FormatProgress {
                    attempt,
                    path,
                    percentage,
                    elapsed,
                } => UpdateStateKind::InstallinatorFormatProgress {
                    attempt,
                    path,
                    percentage,
                    elapsed,
                },
                ProgressEventKind::WriteProgress {
                    attempt,
                    kind,
                    destination,
                    written_bytes,
                    total_bytes,
                    elapsed,
                } => UpdateStateKind::ArtifactWriteProgress {
                    attempt,
                    kind,
                    destination: Some(destination),
                    written_bytes,
                    total_bytes,
                    elapsed,
                },
            }
        } else {
            UpdateStateKind::WaitingForProgress {
                component: "installinator".to_owned(),
            }
        };

        self.set_current_update_state(kind);
    }

    // Installs the installinator phase 1 and configures the host to fetch phase
    // 2 from MGS on boot, returning the image ID of that phase 2 image for use
    // when querying MGS for progress on its delivery to the SP.
    async fn install_trampoline_image(
        &mut self,
        plan: &UpdatePlan,
    ) -> anyhow::Result<HostPhase2RecoveryImageId> {
        // We arbitrarily choose to store the trampoline phase 1 in host boot
        // slot 0.
        let trampoline_phase_1_boot_slot = 0;
        self.deliver_host_phase1(
            &plan.trampoline_phase_1,
            trampoline_phase_1_boot_slot,
        )
        .await?;

        // Wait (if necessary) for the trampoline phase 2 upload to MGS to
        // complete. We started a task to do this the first time a sled update
        // was started with this plan.
        let uploaded_trampoline_phase2_id =
            self.wait_for_upload_tramponline_to_mgs(plan).await?;

        // Set the installinator image ID.
        self.set_current_update_state(
            UpdateStateKind::SettingInstallinatorOptions,
        );
        let installinator_image_id = InstallinatorImageId {
            control_plane: plan.control_plane_hash.to_string(),
            host_phase_2: plan.host_phase_2_hash.to_string(),
            update_id: self.update_id,
        };
        self.mgs_client
            .sp_installinator_image_id_set(
                self.sp.type_,
                self.sp.slot,
                &installinator_image_id,
            )
            .await
            .context("failed to set installinator image ID")?;

        // Ensure we've selected the correct slot from which to boot, and set
        // the host startup option to fetch the trampoline phase 2 from MGS.
        self.set_current_update_state(
            UpdateStateKind::SettingHostStartupOptions,
        );
        self.mgs_client
            .sp_component_active_slot_set(
                self.sp.type_,
                self.sp.slot,
                SpComponent::HOST_CPU_BOOT_FLASH.const_as_str(),
                &SpComponentFirmwareSlot { slot: trampoline_phase_1_boot_slot },
            )
            .await
            .context("failed to set host boot flash slot")?;
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
            .context("failed to set host startup options for recovery mode")?;

        // All set - boot the host and let installinator do its thing!
        self.set_host_power_state(PowerState::A0).await?;

        Ok(uploaded_trampoline_phase2_id)
    }

    async fn install_host_phase_1_and_boot(
        &self,
        plan: &UpdatePlan,
    ) -> anyhow::Result<()> {
        // Installinator is done - set the stage for the real host to boot.

        // Deliver the real host phase 1 image.
        //
        // TODO-correctness This choice of boot slot MUST match installinator.
        // We could install it into both slots (and maybe we should!), but we
        // still need to know which M.2 installinator copied the OS onto so we
        // can set the correct boot device. Thinking out loud: Even if it
        // doesn't do it today, installinator probably wants to _dynamically_
        // choose an M.2 to account for missing or failed drives. Maybe its
        // final completion message should tell us which slot (or both!) it
        // wrote to, and then we echo that choice here?
        let host_phase_1_boot_slot = 0;
        self.deliver_host_phase1(&plan.host_phase_1, host_phase_1_boot_slot)
            .await?;

        // Clear the installinator image ID; failing to do this is _not_ fatal,
        // because any future update will set its own installinator ID anyway;
        // this is for cleanliness more than anything.
        if let Err(err) = self
            .mgs_client
            .sp_installinator_image_id_delete(self.sp.type_, self.sp.slot)
            .await
        {
            warn!(
                self.log,
                "failed to clear installinator image ID (proceeding anyway)";
                "err" => %err,
            );
        }

        // Set the startup options for a standard boot (i.e., no options).
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
                    phase2_recovery_mode: false,
                    prom: false,
                    verbose: false,
                },
            )
            .await
            .context("failed to set host startup options for standard boot")?;

        // Boot the host.
        self.set_host_power_state(PowerState::A0).await?;

        Ok(())
    }

    async fn wait_for_upload_tramponline_to_mgs(
        &mut self,
        plan: &UpdatePlan,
    ) -> anyhow::Result<HostPhase2RecoveryImageId> {
        loop {
            if let Some(image_id) = self
                .upload_trampoline_phase_2_to_mgs
                .borrow()
                .uploaded_image_id
                .as_ref()
            {
                return Ok(image_id.clone());
            }

            // This looks like we might be setting our current state multiple
            // times (since we're in a loop), but in practice we should only
            // loop once: once `self.upload_trampoline_phase_2_to_mgs.changed()`
            // fires, our check above should return the image ID.
            self.set_current_update_state(
                UpdateStateKind::SendingArtifactToMgs {
                    artifact: plan.trampoline_phase_2.id.clone(),
                },
            );

            // `upload_trampoline_phase_2_to_mgs` waits for all
            // receivers, so if `changed()` fails that means the task
            // has either panicked or been cancelled due to a new repo
            // upload - either way, we can't continue.
            self.upload_trampoline_phase_2_to_mgs.changed().await.map_err(
                |_recv_err| {
                    anyhow!(concat!(
                        "failed to upload trampoline phase 2 to MGS ",
                        "(was a new TUF repo uploaded?)"
                    ))
                },
            )?;
        }
    }

    async fn deliver_host_phase1(
        &self,
        artifact: &ArtifactIdData,
        boot_slot: u16,
    ) -> anyhow::Result<()> {
        const HOST_BOOT_FLASH: &str =
            SpComponent::HOST_CPU_BOOT_FLASH.const_as_str();

        let phase1_image = buf_list_to_try_stream(artifact.data.0.clone());

        // Ensure host is in A2.
        self.set_host_power_state(PowerState::A2).await?;

        // Start delivering image.
        let update_id = Uuid::new_v4();
        info!(
            self.log, "sending phase 1 host image";
            "artifact_id" => ?artifact.id,
            "update_id" => %update_id,
        );
        self.set_current_update_state(UpdateStateKind::SendingArtifactToMgs {
            artifact: artifact.id.clone(),
        });
        self.mgs_client
            .sp_component_update(
                self.sp.type_,
                self.sp.slot,
                HOST_BOOT_FLASH,
                boot_slot,
                &update_id,
                reqwest::Body::wrap_stream(phase1_image),
            )
            .await
            .with_context(|| {
                format!("failed to write host boot flash slot {boot_slot}")
            })?;

        // Wait for image delivery to complete.
        info!(
            self.log, "waiting for phase 1 delivery to complete";
            "artifact_id" => ?artifact.id,
            "update_id" => %update_id,
        );
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
        let start = Instant::now();

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
                        UpdateStateKind::ArtifactWriteProgress {
                            // We currently don't do any retries, so this is attempt 1.
                            attempt: 1,
                            kind: artifact.kind.clone(),
                            destination: None,
                            written_bytes: bytes_received.into(),
                            total_bytes: total_bytes.into(),
                            elapsed: start.elapsed(),
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
    let uploaded_image_id = backoff::retry_notify(
        backoff::retry_policy_internal_service_aggressive(),
        upload_task,
        log_failure,
    )
    .await
    .unwrap()
    .into_inner();

    // Notify all receivers that we've uploaded the image.
    _ = status.send(UploadTrampolinePhase2ToMgsStatus {
        id: artifact.id,
        uploaded_image_id: Some(uploaded_image_id),
    });

    // Wait for all receivers to be gone before we exit, so they don't get recv
    // errors unless we're cancelled.
    status.closed().await;
}
