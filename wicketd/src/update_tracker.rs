// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use crate::artifacts::ArtifactIdData;
use crate::artifacts::UpdatePlan;
use crate::installinator_progress::IprStartReceiver;
use crate::installinator_progress::IprUpdateTracker;
use crate::mgs::make_mgs_client;
use crate::update_events::ComponentRegistrar;
use crate::update_events::CurrentProgress;
use crate::update_events::Event;
use crate::update_events::ProgressEvent;
use crate::update_events::ProgressEventKind;
use crate::update_events::SpComponentUpdateStage;
use crate::update_events::StepContext;
use crate::update_events::StepEvent;
use crate::update_events::StepEventKind;
use crate::update_events::StepHandle;
use crate::update_events::StepInfo;
use crate::update_events::StepInfoWithMetadata;
use crate::update_events::StepOutcome;
use crate::update_events::StepProgress;
use crate::update_events::StepResult;
use crate::update_events::UpdateComponent;
use crate::update_events::UpdateEngine;
use crate::update_events::UpdateLog;
use crate::update_events::UpdateStepId;
use crate::update_events::UpdateTerminalError;
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
use installinator_common::ProgressReport;
use omicron_common::backoff;
use omicron_common::update::ArtifactId;
use serde_json::Value;
use slog::error;
use slog::info;
use slog::o;
use slog::warn;
use slog::Logger;
use std::borrow::Cow;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::net::SocketAddrV6;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use update_engine::events::ProgressCounter;
use update_engine::ExecutionId;
use uuid::Uuid;

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

            let update_cx = UpdateContext {
                update_log: Arc::clone(&update_log),
                update_id,
                sp,
                mgs_client: self.mgs_client.clone(),
                upload_trampoline_phase_2_to_mgs,
                log: self.log.new(o!(
                    "sp" => format!("{sp:?}"),
                    "update_id" => update_id.to_string(),
                )),
            };
            // TODO do we need `UpdateDriver` as a distinct type?
            let update_driver = UpdateDriver {};

            let task = tokio::spawn(update_driver.run(
                plan,
                update_cx,
                ipr_start_receiver,
            ));

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
                slot.get().update_log.lock().unwrap().clone()
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
            inner.insert(sp.slot, update_log);
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
struct UpdateDriver {}

impl UpdateDriver {
    async fn run(
        self,
        plan: UpdatePlan,
        update_cx: UpdateContext,
        ipr_start_receiver: IprStartReceiver,
    ) {
        let update_cx = &update_cx;

        // TODO: We currently do updates in the order RoT -> SP -> host. This is
        // generally the correct order, but in some cases there might be a bug
        // which forces us to update components in the order SP -> RoT -> host.
        // How do we handle that?
        //
        // Broadly, there are two ways to do this:
        //
        // 1. Add metadata to artifacts.json indicating the order in which
        //    components should be updated. There are a lot of options in the
        //    design space here, from a simple boolean to a list or DAG
        //    expressing the order, or something even more dynamic than that.
        //
        // 2. Skip updating components that match the same version. This would
        //    let us ship two separate archives in case there's a bug: one with
        //    the newest components for the SP and RoT, and one without.

        // Build the update executor.
        let (sender, mut receiver) = mpsc::channel(128);
        let mut engine = UpdateEngine::new(&update_cx.log, sender);

        let (_rot_artifact, sp_artifact) = match update_cx.sp.type_ {
            SpType::Sled => (&plan.gimlet_rot, &plan.gimlet_sp),
            SpType::Power => (&plan.psc_rot, &plan.psc_sp),
            SpType::Switch => (&plan.sidecar_rot, &plan.sidecar_sp),
        };

        /*
        // TODO: Fetch current running RoT slot; update the other one!
        let mut rot_registrar = engine.for_component(UpdateComponent::Rot);
        self.register_sp_component_steps(
            update_cx,
            &mut rot_registrar,
            rot_artifact,
            SpComponent::ROT.const_as_str(),
            firmware_slot_FIXME,
            Default::default(),
        );
        */

        // The SP only has one updateable firmware slot ("the inactive bank") -
        // we always pass 0.
        let sp_firmware_slot = 0;

        let mut sp_registrar = engine.for_component(UpdateComponent::Sp);
        self.register_sp_component_steps(
            update_cx,
            &mut sp_registrar,
            sp_artifact,
            SpComponent::SP_ITSELF.const_as_str(),
            sp_firmware_slot,
            Default::default(),
        );
        sp_registrar
            .new_step(
                UpdateStepId::ResettingSp,
                "Resetting SP",
                |_cx| async move {
                    update_cx.reset_sp().await.map_err(|error| {
                        UpdateTerminalError::SpResetFailed { error }
                    })?;
                    StepResult::success((), Default::default())
                },
            )
            .register();

        if update_cx.sp.type_ == SpType::Sled {
            self.register_sled_steps(
                update_cx,
                &mut engine,
                &plan,
                ipr_start_receiver,
            );
        }

        // Spawn a task to accept all events from the executing engine.
        //
        // TODO see note in `process_installinator_report()` below: we're racing
        // it for access to `update_log`. We should be the only one with it, but
        // for now we share it to allow installinator reports to go into the log
        // directly.
        let update_log = Arc::clone(&update_cx.update_log);
        let event_receiving_task = tokio::spawn(async move {
            // TODO Is this event receiving handler right? Can we end up seeing
            // `current` set to a progress report for a step that is also
            // present in the completed `.events` list?
            while let Some(event) = receiver.recv().await {
                match event {
                    Event::Step(event) => {
                        let mut update_log = update_log.lock().unwrap();
                        update_log.current =
                            Some(CurrentProgress::WaitingForProgressEvent);
                        update_log.events.push(event);
                    }
                    Event::Progress(event) => {
                        update_log.lock().unwrap().current =
                            Some(CurrentProgress::ProgressEvent(event));
                    }
                }
            }

            // All events received and the engine is complete; we no longer have
            // a "current" progress.
            update_log.lock().unwrap().current = None;
        });

        // Execute the update engine.
        match engine.execute().await {
            Ok(_cx) => (),
            Err(err) => {
                error!(update_cx.log, "update failed"; "err" => %err);
            }
        }

        // Wait for all events to be received and written to the update log.
        event_receiving_task.await.expect("event receiving task panicked");
    }

    fn register_sp_component_steps<'a>(
        &self,
        update_cx: &'a UpdateContext,
        registrar: &mut ComponentRegistrar<'_, 'a>,
        artifact: &'a ArtifactIdData,
        component_name: &'static str,
        firmware_slot: u16,
        step_names: SpComponentUpdateStepNames,
    ) {
        let update_id = Uuid::new_v4();

        registrar
            .new_step(
                UpdateStepId::SpComponentUpdate {
                    stage: SpComponentUpdateStage::Sending,
                },
                step_names.sending.clone(),
                move |_cx| async move {
                    // TODO: we should be able to report some sort of progress
                    // here for the file upload.
                    update_cx
                        .mgs_client
                        .sp_component_update(
                            update_cx.sp.type_,
                            update_cx.sp.slot,
                            component_name,
                            firmware_slot,
                            &update_id,
                            reqwest::Body::wrap_stream(buf_list_to_try_stream(
                                BufList::from_iter([artifact.data.0.clone()]),
                            )),
                        )
                        .await
                        .map_err(|error| {
                            UpdateTerminalError::SpComponentUpdateFailed {
                                stage: SpComponentUpdateStage::Sending,
                                artifact: artifact.id.clone(),
                                error: anyhow!(error),
                            }
                        })?;
                    StepResult::success((), Default::default())
                },
            )
            .register();

        self.register_component_update_completion_steps(
            update_cx,
            registrar,
            &artifact.id,
            update_id,
            component_name,
            step_names,
        );
    }

    fn register_component_update_completion_steps<'a>(
        &self,
        update_cx: &'a UpdateContext,
        registrar: &mut ComponentRegistrar<'_, 'a>,
        artifact: &'a ArtifactId,
        update_id: Uuid,
        component: &'static str,
        step_names: SpComponentUpdateStepNames,
    ) {
        registrar
            .new_step(
                UpdateStepId::SpComponentUpdate {
                    stage: SpComponentUpdateStage::Preparing,
                },
                step_names.preparing,
                move |cx| async move {
                    update_cx
                        .poll_component_update(
                            cx,
                            ComponentUpdateStage::Preparing,
                            update_id,
                            component,
                        )
                        .await
                        .map_err(|error| {
                            UpdateTerminalError::SpComponentUpdateFailed {
                                stage: SpComponentUpdateStage::Preparing,
                                artifact: artifact.clone(),
                                error,
                            }
                        })?;

                    StepResult::success((), Default::default())
                },
            )
            .register();

        registrar
            .new_step(
                UpdateStepId::SpComponentUpdate {
                    stage: SpComponentUpdateStage::Writing,
                },
                step_names.writing,
                move |cx| async move {
                    update_cx
                        .poll_component_update(
                            cx,
                            ComponentUpdateStage::InProgress,
                            update_id,
                            component,
                        )
                        .await
                        .map_err(|error| {
                            UpdateTerminalError::SpComponentUpdateFailed {
                                stage: SpComponentUpdateStage::Writing,
                                artifact: artifact.clone(),
                                error,
                            }
                        })?;

                    StepResult::success((), Default::default())
                },
            )
            .register();
    }

    fn register_sled_steps<'a>(
        &self,
        update_cx: &'a UpdateContext,
        engine: &mut UpdateEngine<'a>,
        plan: &'a UpdatePlan,
        ipr_start_receiver: IprStartReceiver,
    ) {
        let mut host_registrar = engine.for_component(UpdateComponent::Host);
        let image_id_handle = self.register_trampoline_phase1_steps(
            update_cx,
            &mut host_registrar,
            plan,
        );

        let start_handle = host_registrar
            .new_step(
                UpdateStepId::DownloadingInstallinator,
                "Downloading installinator, waiting for it to start",
                move |cx| async move {
                    let image_id = image_id_handle.into_value(cx.token()).await;
                    // The previous step should send this value in.
                    let report_receiver = update_cx
                        .wait_for_first_installinator_progress(
                            &cx,
                            ipr_start_receiver,
                            image_id,
                        )
                        .await
                        .map_err(|error| {
                            UpdateTerminalError::DownloadingInstallinatorFailed { error }
                        })?;

                    StepResult::success(report_receiver, Default::default())
                },
            )
            .register();

        // TODO: this should most likely use nested steps.
        host_registrar
            .new_step(
                UpdateStepId::RunningInstallinator,
                "Running installinator",
                move |cx| async move {
                    let report_receiver =
                        start_handle.into_value(cx.token()).await;
                    update_cx
                        .process_installinator_reports(report_receiver)
                        .await
                        .map_err(|error| {
                            UpdateTerminalError::RunningInstallinatorFailed {
                                error,
                            }
                        })?;

                    StepResult::success((), Default::default())
                },
            )
            .register();

        // Installinator is done: install the host phase 1 that matches the host
        // phase 2 it installed, and boot our newly-recovered sled.
        self.register_install_host_phase1_and_boot_steps(
            update_cx,
            &mut host_registrar,
            plan,
        );
    }

    // Installs the trampoline phase 1 and configures the host to fetch phase
    // 2 from MGS on boot, returning the image ID of that phase 2 image for use
    // when querying MGS for progress on its delivery to the SP.
    fn register_trampoline_phase1_steps<'a>(
        &self,
        update_cx: &'a UpdateContext,
        registrar: &mut ComponentRegistrar<'_, 'a>,
        plan: &'a UpdatePlan,
    ) -> StepHandle<HostPhase2RecoveryImageId> {
        // We arbitrarily choose to store the trampoline phase 1 in host boot
        // slot 0.
        let trampoline_phase_1_boot_slot = 0;

        self.register_deliver_host_phase1_steps(
            update_cx,
            registrar,
            &plan.trampoline_phase_1,
            "trampoline",
            trampoline_phase_1_boot_slot,
        );

        // Wait (if necessary) for the trampoline phase 2 upload to MGS to
        // complete. We started a task to do this the first time a sled update
        // was started with this plan.
        let mut upload_trampoline_phase_2_to_mgs =
            update_cx.upload_trampoline_phase_2_to_mgs.clone();

        let image_id_step_handle = registrar.new_step(
            UpdateStepId::WaitingForTrampolinePhase2Upload,
            "Waiting for trampoline phase 2 upload to MGS",
            move |_cx| async move {
                // We expect this loop to run just once, but iterate just in
                // case the image ID doesn't get populated the first time.
                loop {
                    upload_trampoline_phase_2_to_mgs.changed().await.map_err(
                        |_recv_err| {
                            UpdateTerminalError::TrampolinePhase2UploadFailed
                        }
                    )?;

                    if let Some(image_id) = upload_trampoline_phase_2_to_mgs
                        .borrow()
                        .uploaded_image_id
                        .as_ref()
                    {
                        return StepResult::success(
                            image_id.clone(),
                            Default::default(),
                        );
                    }
                }
            },
        ).register();

        registrar
            .new_step(
                UpdateStepId::SettingInstallinatorImageId,
                "Setting installinator image ID",
                move |_cx| async move {
                    let installinator_image_id = InstallinatorImageId {
                        control_plane: plan.control_plane_hash.to_string(),
                        host_phase_2: plan.host_phase_2_hash.to_string(),
                        update_id: update_cx.update_id,
                    };
                    update_cx
                        .mgs_client
                        .sp_installinator_image_id_set(
                            update_cx.sp.type_,
                            update_cx.sp.slot,
                            &installinator_image_id,
                        )
                        .await
                        .map_err(|error| {
                            // HTTP-ERROR-FULL-CAUSE-CHAIN
                            UpdateTerminalError::SetInstallinatorImageIdFailed {
                                error,
                            }
                        })?;

                    StepResult::success((), Default::default())
                },
            )
            .register();

        registrar
            .new_step(
                UpdateStepId::SettingHostStartupOptions,
                "Setting host startup options",
                move |_cx| async move {
                    update_cx
                        .mgs_client
                        .sp_component_active_slot_set(
                            update_cx.sp.type_,
                            update_cx.sp.slot,
                            SpComponent::HOST_CPU_BOOT_FLASH.const_as_str(),
                            &SpComponentFirmwareSlot {
                                slot: trampoline_phase_1_boot_slot,
                            },
                        )
                        .await
                        .map_err(|error| {
                            UpdateTerminalError::SetHostBootFlashSlotFailed {
                                error,
                            }
                        })?;

                    update_cx
                        .mgs_client
                        .sp_startup_options_set(
                            update_cx.sp.type_,
                            update_cx.sp.slot,
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
                        .map_err(|error| {
                            UpdateTerminalError::SetHostStartupOptionsFailed {
                                description: "recovery mode",
                                error,
                            }
                        })?;

                    StepResult::success((), Default::default())
                },
            )
            .register();

        // All set - boot the host and let installinator do its thing!
        registrar
            .new_step(
                UpdateStepId::SetHostPowerState { state: PowerState::A0 },
                "Setting host power state to A0",
                move |_cx| async move {
                    update_cx.set_host_power_state(PowerState::A0).await
                },
            )
            .register();

        image_id_step_handle
    }

    fn register_install_host_phase1_and_boot_steps<'engine, 'a: 'engine>(
        &self,
        update_cx: &'a UpdateContext,
        registrar: &mut ComponentRegistrar<'engine, 'a>,
        plan: &'a UpdatePlan,
    ) {
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

        self.register_deliver_host_phase1_steps(
            update_cx,
            registrar,
            &plan.host_phase_1,
            "host",
            host_phase_1_boot_slot,
        );

        // Clear the installinator image ID; failing to do this is _not_ fatal,
        // because any future update will set its own installinator ID anyway;
        // this is for cleanliness more than anything.
        registrar.new_step(
            UpdateStepId::ClearingInstallinatorImageId,
            "Clearing installinator image ID",
            move |_cx| async move {
                if let Err(err) = update_cx
                    .mgs_client
                    .sp_installinator_image_id_delete(
                        update_cx.sp.type_,
                        update_cx.sp.slot,
                    )
                    .await
                {
                    warn!(
                        update_cx.log,
                        "failed to clear installinator image ID (proceeding anyway)";
                        "err" => %err,
                    );
                }

                StepResult::success((), Default::default())
            }).register();

        registrar
            .new_step(
                UpdateStepId::SettingHostStartupOptions,
                "Setting startup options for standard boot",
                move |_cx| async move {
                    update_cx
                        .mgs_client
                        .sp_startup_options_set(
                            update_cx.sp.type_,
                            update_cx.sp.slot,
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
                        .map_err(|error| {
                            // HTTP-ERROR-FULL-CAUSE-CHAIN
                            UpdateTerminalError::SetHostStartupOptionsFailed {
                                description: "standard boot",
                                error,
                            }
                        })?;

                    StepResult::success((), Default::default())
                },
            )
            .register();

        // Boot the host.
        registrar
            .new_step(
                UpdateStepId::SetHostPowerState { state: PowerState::A0 },
                "Booting the host",
                |_cx| async {
                    update_cx.set_host_power_state(PowerState::A0).await
                },
            )
            .register();
    }

    fn register_deliver_host_phase1_steps<'a>(
        &self,
        update_cx: &'a UpdateContext,
        registrar: &mut ComponentRegistrar<'_, 'a>,
        artifact: &'a ArtifactIdData,
        kind: &str, // "host" or "trampoline"
        boot_slot: u16,
    ) {
        const HOST_BOOT_FLASH: &str =
            SpComponent::HOST_CPU_BOOT_FLASH.const_as_str();

        registrar
            .new_step(
                UpdateStepId::SetHostPowerState { state: PowerState::A2 },
                "Setting host power state to A2",
                move |_cx| async move {
                    update_cx.set_host_power_state(PowerState::A2).await
                },
            )
            .register();

        let step_names = SpComponentUpdateStepNames::for_host_phase_1(kind);

        self.register_sp_component_steps(
            update_cx,
            registrar,
            artifact,
            HOST_BOOT_FLASH,
            boot_slot,
            step_names,
        );
    }
}

struct UpdateContext {
    update_log: Arc<StdMutex<UpdateLog>>,
    update_id: Uuid,
    sp: SpIdentifier,
    mgs_client: gateway_client::Client,
    upload_trampoline_phase_2_to_mgs:
        watch::Receiver<UploadTrampolinePhase2ToMgsStatus>,
    log: slog::Logger,
}

impl UpdateContext {
    async fn process_installinator_reports<'engine>(
        &self,
        mut ipr_receiver: mpsc::Receiver<ProgressReport>,
    ) -> anyhow::Result<()> {
        // TODO: break up installinator into sections. For now we just
        // group everything together.

        while let Some(report) = ipr_receiver.recv().await {
            // NOTE: This needs to bubble up errors returned by the
            // installinator!
            self.process_installinator_report(report).await;
        }

        // The receiver being closed means that the installinator has completed.

        Ok(())
    }

    async fn wait_for_first_installinator_progress(
        &self,
        cx: &StepContext,
        mut ipr_start_receiver: IprStartReceiver,
        image_id: HostPhase2RecoveryImageId,
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
                    self.poll_trampoline_phase2_progress(cx, &image_id).await;
                }
            }
        }
    }

    /// Polls MGS for the latest trampoline phase 2 progress.
    ///
    /// The naming is somewhat confusing here: the code to fetch the respective
    /// phase 2 is present within all phase 1 ROMs, both host and trampoline.
    /// This is why the API has the name "host phase 2" in it. However, for this
    /// update flow it is only activated for trampoline images.
    async fn poll_trampoline_phase2_progress(
        &self,
        cx: &StepContext,
        uploaded_trampoline_phase2_id: &HostPhase2RecoveryImageId,
    ) {
        match self
            .mgs_client
            .sp_host_phase2_progress_get(self.sp.type_, self.sp.slot)
            .await
            .map(|response| response.into_inner())
        {
            Ok(HostPhase2Progress::Available {
                image_id,
                offset,
                total_size,
                ..
            }) => {
                // Does this image ID match the one we uploaded? If so,
                // record our current progress; if not, this is probably
                // stale data from a past update, and we have no progress
                // information.
                if &image_id == uploaded_trampoline_phase2_id {
                    cx.send_progress(StepProgress::with_current_and_total(
                        offset,
                        total_size,
                        Default::default(),
                    ))
                    .await;
                }
            }
            Ok(HostPhase2Progress::None) => {
                // No progress available -- don't send an update.
                // XXX should we reset the StepProgress to running?
            }
            Err(err) => {
                warn!(
                    self.log, "failed to get SP host phase2 progress";
                    "err" => %err,
                );
            }
        }
    }

    async fn process_installinator_report(&self, mut report: ProgressReport) {
        use installinator_common::ProgressEventKind as InstallinatorProgressEvent;
        // XXX This needs to be rewritten once the installinator switches to the
        // update engine. Note that errors returned by the installinator are not
        // currently bubbled up!

        fn make_step_meta<S: Into<Cow<'static, str>>>(
            description: S,
        ) -> StepInfoWithMetadata {
            StepInfoWithMetadata {
                info: StepInfo {
                    id: UpdateStepId::RunningInstallinator,
                    component: UpdateComponent::Host,
                    description: description.into(),
                    index: 0,                 // TODO?
                    component_index: 0,       // TODO?
                    total_component_steps: 0, // TODO?
                },
                metadata: None,
            }
        }

        // This is a fake execution ID used as a placeholder until the
        // installinator switches to using the update engine.
        let execution_id = ExecutionId(
            "2b1a3f6e-18cc-4b9b-8852-9d31aeb89a39".parse().expect("valid UUID"),
        );

        // Currently, progress reports have zero or one progress events. Don't
        // assert that here, in case this version of wicketd is updating a
        // future installinator which reports multiple progress events.
        let progress_event: ProgressEvent = if let Some(event) =
            report.progress_events.drain(..).next()
        {
            // TODO replace these with nested update engine events?
            match event.kind {
                InstallinatorProgressEvent::DownloadProgress {
                    attempt,
                    kind,
                    peer,
                    downloaded_bytes,
                    total_bytes,
                    elapsed,
                } => ProgressEvent {
                    execution_id,
                    total_elapsed: elapsed,
                    kind: ProgressEventKind::Progress {
                        step: make_step_meta(format!(
                            "installinator downloading {kind:?} from {peer}",
                        )),
                        attempt,
                        metadata: Value::Null,
                        progress: Some(ProgressCounter {
                            current: downloaded_bytes,
                            total: Some(total_bytes),
                        }),
                        step_elapsed: elapsed,
                        attempt_elapsed: elapsed, // TODO
                    },
                },
                InstallinatorProgressEvent::FormatProgress {
                    attempt,
                    path,
                    percentage,
                    elapsed,
                } => ProgressEvent {
                    execution_id,
                    total_elapsed: elapsed,
                    kind: ProgressEventKind::Progress {
                        step: make_step_meta(format!(
                            "installinator formatting {path}",
                        )),
                        attempt,
                        metadata: Value::Null,
                        progress: Some(ProgressCounter {
                            current: percentage as u64,
                            total: Some(100), // TODO ?
                        }),
                        step_elapsed: elapsed,
                        attempt_elapsed: elapsed, // TODO
                    },
                },
                InstallinatorProgressEvent::WriteProgress {
                    attempt,
                    kind,
                    destination,
                    written_bytes,
                    total_bytes,
                    elapsed,
                } => ProgressEvent {
                    execution_id,
                    total_elapsed: elapsed,
                    kind: ProgressEventKind::Progress {
                        step: make_step_meta(format!(
                            "installinator writing {kind:?} to {destination}"
                        )),
                        attempt,
                        metadata: Value::Null,
                        progress: Some(ProgressCounter {
                            current: written_bytes,
                            total: Some(total_bytes),
                        }),
                        step_elapsed: elapsed,
                        attempt_elapsed: elapsed, // TODO
                    },
                },
            }
        } else {
            ProgressEvent {
                execution_id,
                total_elapsed: Duration::default(), // TODO
                kind: ProgressEventKind::Progress {
                    step: make_step_meta("waiting for installinator"),
                    attempt: 0, // TODO
                    metadata: Value::Null,
                    progress: Some(ProgressCounter { current: 0, total: None }),
                    step_elapsed: Duration::default(), // TODO
                    attempt_elapsed: Duration::default(), // TODO
                },
            }
        };

        let events = report.completion_events.into_iter().map(|event| {
            use installinator_common::CompletionEventKind;

            let kind = match event.kind {
                CompletionEventKind::DownloadFailed {
                    attempt,
                    kind,
                    peer,
                    downloaded_bytes: _downloaded_bytes,
                    elapsed,
                    message,
                } => StepEventKind::AttemptRetry {
                    step: make_step_meta(format!(
                        "failed to download {kind:?} from {peer}",
                    )),
                    next_attempt: attempt + 1,
                    step_elapsed: elapsed, // TODO
                    attempt_elapsed: elapsed,
                    message: message.into(),
                },
                CompletionEventKind::DownloadCompleted {
                    attempt,
                    kind,
                    peer,
                    artifact_size: _size,
                    elapsed,
                } => StepEventKind::StepCompleted {
                    step: make_step_meta(format!(
                        "download of {kind:?} from {peer} complete"
                    )),
                    attempt,
                    outcome: StepOutcome::Success { metadata: Value::Null },
                    next_step: make_step_meta("TODO FIXME"), // WRONG
                    step_elapsed: elapsed,                   // TODO
                    attempt_elapsed: elapsed,                // TODO
                },
                CompletionEventKind::FormatFailed {
                    attempt,
                    path,
                    elapsed,
                    message,
                } => StepEventKind::AttemptRetry {
                    step: make_step_meta(format!("failed to format {path}")),
                    next_attempt: attempt + 1,
                    step_elapsed: elapsed, // TODO
                    attempt_elapsed: elapsed,
                    message: message.into(),
                },
                CompletionEventKind::FormatCompleted {
                    attempt,
                    path,
                    elapsed,
                } => StepEventKind::StepCompleted {
                    step: make_step_meta(format!("format of {path} complete")),
                    attempt,
                    outcome: StepOutcome::Success { metadata: Value::Null },
                    next_step: make_step_meta("TODO FIXME"), // WRONG
                    step_elapsed: elapsed,                   // TODO
                    attempt_elapsed: elapsed,                // TODO
                },
                CompletionEventKind::WriteFailed {
                    attempt,
                    kind,
                    destination,
                    written_bytes: _written_bytes,
                    total_bytes: _total_bytes,
                    elapsed,
                    message,
                } => StepEventKind::AttemptRetry {
                    step: make_step_meta(format!(
                        "failed to write {kind:?} to {destination}"
                    )),
                    next_attempt: attempt + 1,
                    step_elapsed: elapsed, // TODO
                    attempt_elapsed: elapsed,
                    message: message.into(),
                },
                CompletionEventKind::WriteCompleted {
                    attempt,
                    kind,
                    destination,
                    artifact_size: _size,
                    elapsed,
                } => StepEventKind::StepCompleted {
                    step: make_step_meta(format!(
                        "write of {kind:?} to {destination} complete"
                    )),
                    attempt,
                    outcome: StepOutcome::Success { metadata: Value::Null },
                    next_step: make_step_meta("TODO FIXME"), // WRONG
                    step_elapsed: elapsed,                   // TODO
                    attempt_elapsed: elapsed,                // TODO
                },
                CompletionEventKind::MiscError {
                    operation,
                    attempt,
                    data: _data,
                    elapsed,
                    message,
                    is_fatal,
                } => {
                    let meta = make_step_meta(format!("error on {operation}"));
                    if is_fatal {
                        // TODO us creating this is suspect! but we'll leave it
                        // for now and remove it soon with better substep
                        // integration
                        StepEventKind::ExecutionFailed {
                            failed_step: meta,
                            total_attempts: attempt,
                            step_elapsed: elapsed, // TODO
                            attempt_elapsed: elapsed,
                            message,
                            causes: Vec::new(),
                        }
                    } else {
                        StepEventKind::AttemptRetry {
                            step: meta,
                            next_attempt: attempt + 1,
                            step_elapsed: elapsed, // TODO
                            attempt_elapsed: elapsed,
                            message: message.into(),
                        }
                    }
                }
                CompletionEventKind::Completed => {
                    StepEventKind::StepCompleted {
                        step: make_step_meta("installinator complete"),
                        attempt: 1, // TODO
                        outcome: StepOutcome::Success { metadata: Value::Null },
                        next_step: make_step_meta("TODO FIXME"), // WRONG
                        step_elapsed: Duration::default(),       // TODO
                        attempt_elapsed: Duration::default(),    // TODO
                    }
                }
            };

            StepEvent { execution_id, total_elapsed: event.total_elapsed, kind }
        });

        // TODO: This races with our task that receives events from the update
        // engine in `run()` above. Once we integrate installinator sub-steps we
        // should remove `update_log` from `UpdateContext` altogether, and let
        // just the event-receiving task hold it.
        let mut update_log = self.update_log.lock().unwrap();
        update_log.current =
            Some(CurrentProgress::ProgressEvent(progress_event));
        for event in events {
            update_log.events.push(event);
        }
    }

    async fn set_host_power_state(
        &self,
        power_state: PowerState,
    ) -> Result<StepResult<()>, UpdateTerminalError> {
        info!(self.log, "moving host to {power_state:?}");
        self.mgs_client
            .sp_power_state_set(self.sp.type_, self.sp.slot, power_state)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| UpdateTerminalError::UpdatePowerStateFailed {
                error,
            })?;
        StepResult::success((), Default::default())
    }

    async fn reset_sp(&self) -> anyhow::Result<()> {
        self.mgs_client
            .sp_reset(self.sp.type_, self.sp.slot)
            .await
            .context("failed to reset SP")
            .map(|res| res.into_inner())
    }

    async fn poll_component_update(
        &self,
        cx: StepContext,
        stage: ComponentUpdateStage,
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
                    bail!("SP no longer processing update (did it reset?)")
                }
                SpUpdateStatus::Preparing { id, progress } => {
                    ensure!(id == update_id, "SP processing different update");
                    if stage == ComponentUpdateStage::Preparing {
                        if let Some(progress) = progress {
                            cx.send_progress(
                                StepProgress::with_current_and_total(
                                    progress.current as u64,
                                    progress.total as u64,
                                    Default::default(),
                                ),
                            )
                            .await;
                        }
                    } else {
                        warn!(
                            self.log,
                            "component update moved backwards \
                             from {stage:?} to preparing"
                        );
                    }
                }
                SpUpdateStatus::InProgress {
                    bytes_received,
                    id,
                    total_bytes,
                } => {
                    ensure!(id == update_id, "SP processing different update");
                    match stage {
                        ComponentUpdateStage::Preparing => {
                            // The prepare step is done -- exit this loop and move
                            // to the next stage.
                            return Ok(());
                        }
                        ComponentUpdateStage::InProgress => {
                            cx.send_progress(
                                StepProgress::with_current_and_total(
                                    bytes_received as u64,
                                    total_bytes as u64,
                                    Default::default(),
                                ),
                            )
                            .await;
                        }
                    }
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

struct SpComponentUpdateStepNames {
    sending: Cow<'static, str>,
    preparing: Cow<'static, str>,
    writing: Cow<'static, str>,
}

impl SpComponentUpdateStepNames {
    fn for_host_phase_1(kind: &str) -> Self {
        Self {
            sending: format!("Sending {kind} phase 1 image to MGS").into(),
            preparing: format!("Preparing to receive {kind} phase 1 update")
                .into(),
            writing: format!("Writing {kind} phase 1 update").into(),
        }
    }
}

impl Default for SpComponentUpdateStepNames {
    fn default() -> Self {
        Self {
            sending: "Sending artifact to MGS".into(),
            preparing: "Preparing to receive update".into(),
            writing: "Writing update".into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ComponentUpdateStage {
    Preparing,
    InProgress,
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
        let image =
            buf_list_to_try_stream(BufList::from_iter([data.0.clone()]));

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
