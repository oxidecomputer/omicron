// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Drive one or more in-progress MGS-managed updates

use crate::{
    MgsClients, RotUpdater, SpUpdater,
    common_sp_update::{
        ReconfiguratorSpComponentUpdater, STATUS_POLL_INTERVAL, VersionStatus,
    },
    mgs_clients::GatewayClientError,
};
use debug_ignore::DebugIgnore;
use futures::{
    FutureExt, StreamExt, future::BoxFuture, stream::FuturesUnordered,
};
use gateway_client::types::{RotSlot, SpUpdateStatus};
use nexus_types::{deployment::PendingMgsUpdate, inventory::BaseboardId};
use slog::{debug, error, info, o, warn};
use slog_error_chain::InlineErrorChain;
use std::sync::Arc;
use std::{
    collections::{BTreeMap, btree_map::Entry},
    time::{Duration, Instant},
};
use thiserror::Error;
use tokio::sync::watch;
use tufaceous_artifact::KnownArtifactKind;
use uuid::Uuid;

/// How long may the status remain unchanged without us treating this as a
/// problem?
const PROGRESS_TIMEOUT: Duration = Duration::from_secs(120);

/// How long to wait between failed attempts to reset the device
const RESET_DELAY_INTERVAL: Duration = Duration::from_secs(10);

enum DeliveryWaitStatus {
    NotRunning,
    Aborted(Uuid),
    Completed(Uuid),
    StuckUpdating(Uuid), // XXX-dap more details
    Failed(Uuid, String),
}

#[derive(Debug, Error)]
enum DeliveryWaitError {
    #[error("error communicating with MGS")]
    MgsCommunication(#[from] GatewayClientError),
}

async fn wait_for_delivery(
    mgs_clients: &mut MgsClients,
    updater: &(dyn ReconfiguratorSpComponentUpdater + Send + Sync),
) -> Result<DeliveryWaitStatus, DeliveryWaitError> {
    let mut last_status = None;
    let mut last_progress = Instant::now();
    let log = updater.logger();
    let sp_type = updater.target_sp_type();
    let sp_slot = updater.target_sp_slot();
    let component = updater.component();

    loop {
        // XXX-dap don't want to bail out if all MgsClients fail?
        let status = mgs_clients
            .try_all_serially(log, |client| async move {
                let update_status = client
                    .sp_component_update_status(sp_type, sp_slot, component)
                    .await?;

                debug!(
                    updater.logger(), "got update status";
                    "mgs_addr" => client.baseurl(),
                    "status" => ?update_status,
                );

                Ok(update_status)
            })
            .await?
            .into_inner();

        match status {
            SpUpdateStatus::None => {
                return Ok(DeliveryWaitStatus::NotRunning);
            }
            SpUpdateStatus::Preparing { id, .. }
            | SpUpdateStatus::InProgress { id, .. } => {
                if let Some(last) = last_status.replace(status.clone()) {
                    if last == status {
                        if last_progress.elapsed() > PROGRESS_TIMEOUT {
                            error!(
                                log,
                                "progress timeout";
                                "status" => ?status,
                                // XXX-dap formatting
                                "timeout" => ?PROGRESS_TIMEOUT,
                            );

                            return Ok(DeliveryWaitStatus::StuckUpdating(id));
                        }
                    } else {
                        last_progress = Instant::now();
                    }
                }
            }
            SpUpdateStatus::Complete { id } => {
                return Ok(DeliveryWaitStatus::Completed(id));
            }
            SpUpdateStatus::Aborted { id } => {
                return Ok(DeliveryWaitStatus::Aborted(id));
            }
            SpUpdateStatus::Failed { code, id } => {
                return Ok(DeliveryWaitStatus::Failed(
                    id,
                    format!("code {code}"),
                ));
            }
            SpUpdateStatus::RotError { id, message } => {
                return Ok(DeliveryWaitStatus::Failed(
                    id,
                    format!("RoT error: {message}"),
                ));
            }
        }

        tokio::time::sleep(STATUS_POLL_INTERVAL).await;
    }
}

// XXX-dap oh no what if somebody else updated it in a newer blueprint
// maybe one solution here is that PendingMgsUpdate records the expected current
// version?  that way if it's changed, we won't proceed.  requires extra
// blueprint work in planner to fix up if things do change.

#[derive(Debug)]
pub enum ApplyUpdateResult {
    PreconditionFailed, // XXX-dap more details
    Completed,
    Aborted(Uuid),
    Failed(Uuid, String),
    ResetFailed(String),
    StuckUpdating(Uuid),  // XXX-dap more details
    StuckResetting(Uuid), // XXX-dap more details
    Lost,
}

#[derive(Debug, Error)]
pub enum ApplyUpdateError {
    #[error("error communicating with MGS")]
    MgsCommunication(#[from] GatewayClientError),
    #[error(
        "component found in indeterminate state (neither old version nor \
         new version)"
    )]
    Indeterminate,
}

impl From<DeliveryWaitError> for ApplyUpdateError {
    fn from(value: DeliveryWaitError) -> Self {
        match value {
            DeliveryWaitError::MgsCommunication(error) => {
                // XXX-dap add more context?
                ApplyUpdateError::MgsCommunication(error)
            }
        }
    }
}

pub async fn apply_update(
    updater: &(dyn ReconfiguratorSpComponentUpdater + Send + Sync),
    mgs_clients: &mut MgsClients,
    update: &PendingMgsUpdate,
) -> Result<ApplyUpdateResult, ApplyUpdateError> {
    // XXX-dap need some component-specific way to determine:
    // - this is not yet updated but ready for update
    // - this is updated
    // - this is neither
    // e.g., for RoT, it's: get_component_active_slot() +
    // sp_component_caboose_get() + parse caboose
    // e.g., for SP, it's sp_component_caboose_get() + parse caboose
    // e.g., for RoT bootloader, it's: maybe sp_rot_boot_info() +
    // sp_component_caboose_get() + caboose parse (with some logic around other
    // stuff)
    // e.g., for host OS it's: unclear?
    match updater.version_status(mgs_clients, update).await? {
        VersionStatus::UpdateComplete => {
            return Ok(ApplyUpdateResult::Completed);
        }
        VersionStatus::NotReadyForUpdate => {
            return Ok(ApplyUpdateResult::PreconditionFailed);
        }
        VersionStatus::ReadyForUpdate => (),
    };

    // Start the update.
    let log = updater.logger();
    let sp_type = updater.target_sp_type();
    let sp_slot = updater.target_sp_slot();
    let component = updater.component();
    let my_update_id = updater.update_id();

    let update_start = mgs_clients
        .try_all_serially(log, |client| async move {
            client
                .sp_component_update(
                    sp_type,
                    sp_slot,
                    component,
                    updater.firmware_slot(),
                    &updater.update_id(),
                    reqwest::Body::from(updater.update_data()),
                )
                .await?;
            info!(
                updater.logger(), "update started";
                "mgs_addr" => client.baseurl(),
            );
            Ok(())
        })
        .await;

    match update_start {
        Ok(()) => (),
        // XXX-dap need to handle errors here.
        // - non-transient error that "an update is already ongoing"
        //  *definitely* needs to be interpreted and allowed to proceed normally
        // - transient errors should be retried in `try_all_serially()`, I
        //   think?  Is it safe to bail out if this happens?  I guess it
        //   probably is if things just get stuck.
        // - other non-transient errors probably mean bailing out
        Err(_) => todo!(),
    };

    let our_update = match wait_for_delivery(mgs_clients, updater).await? {
        DeliveryWaitStatus::NotRunning => return Ok(ApplyUpdateResult::Lost),
        DeliveryWaitStatus::Aborted(id) => {
            return Ok(ApplyUpdateResult::Aborted(id));
        }
        DeliveryWaitStatus::StuckUpdating(id) => {
            return Ok(ApplyUpdateResult::StuckUpdating(id));
        }
        DeliveryWaitStatus::Failed(id, message) => {
            return Ok(ApplyUpdateResult::Failed(id, message));
        }

        DeliveryWaitStatus::Completed(id) => id == my_update_id,
    };

    // If we were the one doing the update, then we're responsible for
    // any post-update action (generally, resetting the device).
    //
    // Regardless of whether we were the one doing the update, we want to wait
    // up to PROGRESS_TIMEOUT for it to come back on the new version.
    //
    // If we were *not* the one doing the update, and it doesn't come back on
    // the new version after PROGRESS_TIMEOUT, then we assume the Nexus doing
    // the update may be stuck or crashed and we *do* want to issue the reset
    // ourselves.
    let try_reset = if our_update {
        true
    } else {
        match wait_for_update_done(
            updater,
            mgs_clients,
            update,
            Some(PROGRESS_TIMEOUT),
        )
        .await
        {
            Ok(_) => false,
            Err(UpdateWaitError::Timeout(_)) => {
                warn!(log, "update takeover: sending reset");
                true
            }
            Err(UpdateWaitError::Indeterminate) => {
                return Err(ApplyUpdateError::Indeterminate);
            }
        }
    };

    if try_reset {
        // We retry this until we get some error *other* than a communication
        // error.  There is intentionally no timeout here.  If we've staged an
        // update but not managed to reset the device, there's no point where
        // we'd want to stop trying to do so.
        while let Err(error) = updater.post_update(mgs_clients, update).await {
            if !matches!(error, gateway_client::Error::CommunicationError(_)) {
                let error = InlineErrorChain::new(&error);
                error!(log, "post_update failed"; &error);
                return Ok(ApplyUpdateResult::ResetFailed(error.to_string()));
            }

            tokio::time::sleep(RESET_DELAY_INTERVAL).await;
        }
    }

    // Regardless of whether it was our job to reset the device, wait for it to
    // come back on the new version.
    //
    // There's no point where we'd want to give up here.  We know a reset was
    // sent.
    match wait_for_update_done(updater, mgs_clients, update, None).await {
        // We did not specify a timeout so it should not time out.
        Err(UpdateWaitError::Timeout(_)) => unreachable!(),
        Ok(()) => {
            info!(log, "update completed");
            Ok(ApplyUpdateResult::Completed)
        }
        Err(UpdateWaitError::Indeterminate) => {
            Err(ApplyUpdateError::Indeterminate)
        }
    }

    // XXX-dap then wrap this whole thing with a wrapper that runs it in a loop,
    // aborting the failure cases, and finishing when it's done
}

#[derive(Debug, Error)]
enum UpdateWaitError {
    #[error("timed out after {0:?}")]
    Timeout(Duration),
    #[error("found to be running neither previous version nor old version")]
    Indeterminate,
}

async fn wait_for_update_done(
    updater: &(dyn ReconfiguratorSpComponentUpdater + Send + Sync),
    mgs_clients: &mut MgsClients,
    update: &PendingMgsUpdate,
    timeout: Option<Duration>,
) -> Result<(), UpdateWaitError> {
    let before = Instant::now();

    // We retry this until we get some error *other* than a communication
    // error.  There is intentionally no timeout here.  If we've staged an
    // update but not managed to reset the device, there's no point where
    // we'd want to stop trying to do so.

    loop {
        match updater.version_status(mgs_clients, update).await {
            Ok(VersionStatus::UpdateComplete) => return Ok(()),
            Ok(VersionStatus::NotReadyForUpdate) => {
                return Err(UpdateWaitError::Indeterminate);
            }
            Err(_) | Ok(VersionStatus::ReadyForUpdate) => {
                match timeout {
                    Some(timeout) if before.elapsed() >= timeout => {
                        return Err(UpdateWaitError::Timeout(timeout));
                    }
                    _ => {
                        // XXX-dap constant name
                        tokio::time::sleep(RESET_DELAY_INTERVAL).await;
                        continue;
                    }
                };
            }
        }
    }
}

struct UpdateAttemptResult {
    requested_update: PendingMgsUpdate,
    result: Result<ApplyUpdateResult, ApplyUpdateError>,
}

#[derive(Debug)]
pub struct MgsUpdateRequest {
    requested_update: PendingMgsUpdate,
    data: DebugIgnore<Arc<Vec<u8>>>,
    // XXX-dap rather than this, MgsClients probably ought to have a mode where
    // it accepts a qorb pool and continually try all clients forever on
    // transient issues
    mgs_clients: MgsClients,
}

// XXX-dap
type InProgressUpdateStatus = ();

struct InProgressUpdate {
    log: slog::Logger,
    status_tx: watch::Sender<InProgressUpdateStatus>,
    status_rx: watch::Receiver<InProgressUpdateStatus>,
}

pub struct MgsUpdateDriver {
    log: slog::Logger,
    requests: watch::Receiver<BTreeMap<BaseboardId, MgsUpdateRequest>>,
    // XXX-dap fill in status here
    in_progress: BTreeMap<BaseboardId, InProgressUpdate>,
    futures: FuturesUnordered<BoxFuture<'static, UpdateAttemptResult>>,
}

impl MgsUpdateDriver {
    pub fn new(
        log: slog::Logger,
        rx: watch::Receiver<BTreeMap<BaseboardId, MgsUpdateRequest>>,
    ) -> MgsUpdateDriver {
        MgsUpdateDriver {
            log,
            requests: rx,
            in_progress: BTreeMap::new(),
            futures: FuturesUnordered::new(),
        }
    }

    pub async fn run(mut self) {
        info!(&self.log, "starting MgsUpdateDriver");
        loop {
            tokio::select! {
                // XXX-dap copy comment from steno sec.rs L831 or so
                maybe_work_done = self.futures.next(),
                    if !self.futures.is_empty() => {
                    match maybe_work_done {
                        Some(result) => self.attempt_done(result),
                        None => {
                            error!(
                                &self.log,
                                "FutureUnordered unexpectedly ended"
                            );
                            break;
                        }
                    };
                },

                maybe_update = self.requests.changed() => {
                    match maybe_update {
                        Ok(()) => {
                            self.update_requests();
                        }
                        Err(error) => {
                            error!(
                                &self.log,
                                "failed to read from input channel";
                                InlineErrorChain::new(&error)
                            );
                            break;
                        }
                    }
                }
            }
        }
    }

    fn attempt_done(&mut self, result: UpdateAttemptResult) {
        let in_progress = self
            .in_progress
            .remove(&result.requested_update.baseboard_id)
            .expect("in-progress record for attempt that just completed");

        // XXX-dap record ringbuffer of attempts
        info!(
            &in_progress.log,
            "update attempt done";
            "result" => ?result.result
        );

        // Re-dispatch this update if either of these is true:
        //
        // - the request is unchanged and the attempt was not successful
        // - the request is changed and not `None`
        let requests = self.requests.borrow();
        let maybe_new_plan =
            requests.get(&result.requested_update.baseboard_id);
        match (maybe_new_plan, result.result) {
            (None, _) => {
                info!(
                    &in_progress.log,
                    "no retry needed (update no longer wanted)"
                );
            }
            (Some(new_plan), Ok(ApplyUpdateResult::Completed))
                if new_plan.requested_update == result.requested_update =>
            {
                info!(&in_progress.log, "no retry needed (converged)");
            }
            (Some(new_plan), _) => {
                info!(
                    &in_progress.log,
                    "retry needed (plan changed or last attempt did not succeed)"
                );
                let work = Self::dispatch_update(self.log.clone(), new_plan);
                drop(requests);
                self.do_dispatch(
                    result.requested_update.baseboard_id.clone(),
                    work,
                );
            }
        };
    }

    fn update_requests(&mut self) {
        // Kick off updates for any newly-added requests.
        //
        // We don't need to do anything with newly-removed requests or changed
        // requests.  We're not going to cancel them.  Once the current attempt
        // completes, we'll stop working on them.
        let new_requests = self.requests.borrow_and_update();
        let mut work_items = Vec::new();
        for (baseboard_id, request) in new_requests.iter() {
            match self.in_progress.entry(baseboard_id.clone()) {
                Entry::Occupied(_) => {
                    info!(
                        &self.log,
                        "update requested for baseboard with update already \
                         in progress";
                        &baseboard_id,
                        "request" => ?request,
                    );
                }
                Entry::Vacant(_) => {
                    work_items.push((
                        baseboard_id.clone(),
                        Self::dispatch_update(self.log.clone(), request),
                    ));
                }
            }
        }
        drop(new_requests);

        for (baseboard_id, work) in work_items {
            self.do_dispatch(baseboard_id.clone(), work);
        }
    }

    fn do_dispatch(
        &mut self,
        baseboard_id: BaseboardId,
        what: Option<(
            InProgressUpdate,
            BoxFuture<'static, UpdateAttemptResult>,
        )>,
    ) {
        if let Some((in_progress, future)) = what {
            self.in_progress.insert(baseboard_id.clone(), in_progress);
            self.futures.push(future);
        }
    }

    fn dispatch_update(
        log: slog::Logger,
        request: &MgsUpdateRequest,
    ) -> Option<(InProgressUpdate, BoxFuture<'static, UpdateAttemptResult>)>
    {
        let log =
            log.new(o!("update" => format!("{:?}", request.requested_update)));
        let update = &request.requested_update;
        let raw_kind = &update.artifact_hash_id.kind;
        let Some(known_kind) = raw_kind.to_known() else {
            error!(
                &log,
                "ignoring update requested for unknown artifact kind: {:?}",
                update.artifact_hash_id.kind,
            );
            return None;
        };

        // XXX-dap check sp_type against artifact kind
        let update_id = Uuid::new_v4();
        let data: Vec<u8> = (*request.data.0).clone();
        let updater: Box<dyn ReconfiguratorSpComponentUpdater + Send + Sync> =
            match known_kind {
                KnownArtifactKind::GimletSp
                | KnownArtifactKind::PscSp
                | KnownArtifactKind::SwitchSp => Box::new(SpUpdater::new(
                    update.sp_type,
                    update.slot_id,
                    update_id,
                    data,
                    &log,
                )),

                KnownArtifactKind::GimletRot
                | KnownArtifactKind::PscRot
                | KnownArtifactKind::SwitchRot => {
                    // XXX-dap should this be done at planning-time?
                    let slot = match update.firmware_slot {
                        0 => RotSlot::A,
                        1 => RotSlot::B,
                        _ => {
                            error!(
                                &log,
                                "ignoring update requested for unknown RoT slot: \
                             {:?}",
                                update
                            );
                            return None;
                        }
                    };
                    Box::new(RotUpdater::new(
                        update.sp_type,
                        update.slot_id,
                        slot,
                        update_id,
                        data,
                        &log,
                    ))
                }

                // XXX-dap should we have checked this earlier?
                KnownArtifactKind::GimletRotBootloader
                | KnownArtifactKind::Host
                | KnownArtifactKind::Trampoline
                | KnownArtifactKind::ControlPlane
                | KnownArtifactKind::Zone
                | KnownArtifactKind::PscRotBootloader
                | KnownArtifactKind::SwitchRotBootloader => {
                    error!(
                        &log,
                        "ignoring update requested for unsupported artifact \
                         kind: {:?}",
                        known_kind,
                    );
                    return None;
                }
            };

        info!(
            &log,
            "update requested for baseboard";
            "request" => ?request
        );

        let (status_tx, status_rx) = watch::channel(());
        let in_progress = InProgressUpdate { log, status_tx, status_rx };
        let mut mgs_clients = request.mgs_clients.clone();
        let update = update.clone();
        let future = async move {
            // XXX-dap clones
            let result =
                apply_update(&*updater, &mut mgs_clients, &update).await;
            UpdateAttemptResult { requested_update: update, result }
        }
        .boxed();
        Some((in_progress, future))
    }
}
