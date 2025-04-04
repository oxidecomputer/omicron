// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Drive one or more in-progress MGS-managed updates

use crate::{
    ArtifactCache, ArtifactCacheError, MgsClients,
    common_sp_update::{
        ReconfiguratorSpComponentUpdater, STATUS_POLL_INTERVAL, VersionStatus,
    },
    mgs_clients::GatewayClientError,
    rot_updater::ReconfiguratorRotUpdater,
    sp_updater::ReconfiguratorSpUpdater,
};
use futures::{
    FutureExt, StreamExt, future::BoxFuture, stream::FuturesUnordered,
};
use gateway_client::{
    SpComponent,
    types::{SpType, SpUpdateStatus},
};
use nexus_types::{deployment::PendingMgsUpdate, inventory::BaseboardId};
use qorb::resolver::{AllBackends, BoxedResolver};
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

/// Drive one or more MGS-managed updates
///
/// Use [`MgsUpdateDriver::new()`] to create a new one of these.  You configure
/// the set of updates that should be driven by writing to its watch channel.
///
/// - If a requested update is added to the channel, an attempt will be made to
///   apply the update promptly.
/// - For each update attempt:
///   - If the live system state reflects that the update has been completed,
///     then no action is taken.
///   - If the live system state reflects that the preconditions aren't true
///     (e.g., the device is currently running from a different slot than
///     expected), then no action is taken.
///   - If the live system state reflects that an update is already in progress,
///     we'll wait for that one to complete.
///   - If the process appears to get stuck, it will be aborted.  This check is
///     generous.  It's really only intended to catch pathological cases like a
///     partitioned Nexus.
///   - Once the update attempt completes, whether it succeeds or fails or just
///     waited for an existing in-progress update, the live state is
///     re-evaluated.  If another attempt is needed, it will be kicked off.
/// - If an update is ongoing when it gets removed from the channel, it is *not*
///   cancelled.  It will run to completion.  But it will not be restarted again
///   even if it fails.
///
/// It's assumed that other instances may exist running the same process.  We
/// may find their updates in progress, we may need to cancel them if they
/// appear stuck, and ours may get cancelled at any point.  These are presumed
/// to be very unlikely.
pub struct MgsUpdateDriver {
    log: slog::Logger,
    artifacts: Arc<ArtifactCache>,
    requests: watch::Receiver<BTreeMap<BaseboardId, PendingMgsUpdate>>,
    // XXX-dap fill in status here
    in_progress: BTreeMap<BaseboardId, InProgressUpdate>,
    futures: FuturesUnordered<BoxFuture<'static, UpdateAttemptResult>>,
    // XXX-dap who calls terminate on this?  when?
    mgs_resolver: BoxedResolver,
    mgs_rx: watch::Receiver<AllBackends>,
}

impl MgsUpdateDriver {
    pub fn new(
        log: slog::Logger,
        artifacts: Arc<ArtifactCache>,
        rx: watch::Receiver<BTreeMap<BaseboardId, PendingMgsUpdate>>,
        mut mgs_resolver: BoxedResolver,
    ) -> MgsUpdateDriver {
        let mgs_rx = mgs_resolver.monitor();

        MgsUpdateDriver {
            log,
            artifacts,
            requests: rx,
            in_progress: BTreeMap::new(),
            futures: FuturesUnordered::new(),
            mgs_resolver,
            mgs_rx,
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
                if *new_plan == result.requested_update =>
            {
                info!(&in_progress.log, "no retry needed (converged)");
            }
            (Some(new_plan), _) => {
                info!(
                    &in_progress.log,
                    "retry needed (plan changed or last attempt did not succeed)"
                );
                let work = Self::dispatch_update(
                    self.log.clone(),
                    self.artifacts.clone(),
                    self.mgs_rx.clone(),
                    new_plan,
                );
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
                        Self::dispatch_update(
                            self.log.clone(),
                            self.artifacts.clone(),
                            self.mgs_rx.clone(),
                            request,
                        ),
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
        artifacts: Arc<ArtifactCache>,
        mgs_rx: watch::Receiver<AllBackends>,
        update: &PendingMgsUpdate,
    ) -> Option<(InProgressUpdate, BoxFuture<'static, UpdateAttemptResult>)>
    {
        let log = log.new(o!("update" => format!("{:?}", update)));
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
        let (sp_update, updater): (
            SpComponentUpdate,
            Box<dyn ReconfiguratorSpComponentUpdater + Send + Sync>,
        ) = match known_kind {
            KnownArtifactKind::GimletSp
            | KnownArtifactKind::PscSp
            | KnownArtifactKind::SwitchSp => {
                let sp_update = SpComponentUpdate {
                    log: log.clone(),
                    component: SpComponent::SP_ITSELF,
                    target_sp_type: update.sp_type,
                    target_sp_slot: update.slot_id,
                    // The SP has two firmware slots, but they're aren't
                    // individually labled. We always request an update to slot
                    // 0, which means "the inactive slot".
                    firmware_slot: 0,
                    update_id,
                };

                (sp_update, Box::new(ReconfiguratorSpUpdater {}))
            }

            KnownArtifactKind::GimletRot
            | KnownArtifactKind::PscRot
            | KnownArtifactKind::SwitchRot => {
                let sp_update = SpComponentUpdate {
                    log: log.clone(),
                    component: SpComponent::ROT,
                    target_sp_type: update.sp_type,
                    target_sp_slot: update.slot_id,
                    firmware_slot: update.firmware_slot,
                    update_id,
                };

                (sp_update, Box::new(ReconfiguratorRotUpdater {}))
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
            "request" => ?update,
        );

        let (status_tx, status_rx) = watch::channel(());
        let in_progress = InProgressUpdate { log, status_tx, status_rx };
        let update = update.clone();
        let future = async move {
            // XXX-dap clones
            let result =
                apply_update(artifacts, &sp_update, &*updater, mgs_rx, &update)
                    .await;
            UpdateAttemptResult { requested_update: update, result }
        }
        .boxed();
        Some((in_progress, future))
    }
}

struct InProgressUpdate {
    log: slog::Logger,
    status_tx: watch::Sender<InProgressUpdateStatus>,
    status_rx: watch::Receiver<InProgressUpdateStatus>,
}

// XXX-dap
type InProgressUpdateStatus = ();

struct UpdateAttemptResult {
    requested_update: PendingMgsUpdate,
    result: Result<ApplyUpdateResult, ApplyUpdateError>,
}

/// Parameters describing a request to update one SP-managed component
// XXX-dap TODO-doc this is very similar to SpComponentUpdater but it uses a
// struct-based interface instead of a trait.
pub struct SpComponentUpdate {
    log: slog::Logger,
    component: SpComponent,
    target_sp_type: SpType,
    target_sp_slot: u32,
    firmware_slot: u16,
    update_id: Uuid,
}

impl SpComponentUpdate {
    fn component(&self) -> &str {
        self.component.const_as_str()
    }
}

enum DeliveryWaitStatus {
    /// the SP does not know about the update we're waiting for
    ///
    /// This can happen if the SP was reset, possibly because the update
    /// completed successfully.
    NotRunning,
    /// the SP reports that this update was aborted
    Aborted(Uuid),
    /// the SP reports that this update was completed
    Completed(Uuid),
    /// the SP reports that this update failed
    Failed(Uuid, String),
    /// we gave up because the update stopped making forward progress for too
    /// long
    StuckUpdating(Uuid), // XXX-dap more details
}

#[derive(Debug, Error)]
enum DeliveryWaitError {
    #[error("error communicating with MGS")]
    MgsCommunication(#[from] GatewayClientError),
}

/// Waits for the delivery (upload) phase of the specified update to complete
///
/// This is used both when we're the one doing the update and when some other
/// component is doing it.
///
/// This returns early if the update stops for any reason or stops making
/// forward progress for too long.
async fn wait_for_delivery(
    mgs_clients: &mut MgsClients,
    update: &SpComponentUpdate,
) -> Result<DeliveryWaitStatus, DeliveryWaitError> {
    let mut last_status = None;
    let mut last_progress = Instant::now();
    let log = &update.log;
    let sp_type = update.target_sp_type;
    let sp_slot = update.target_sp_slot;
    let component = update.component();

    loop {
        // XXX-dap don't want to bail out if all MgsClients fail?
        let status = mgs_clients
            .try_all_serially(log, |client| async move {
                let update_status = client
                    .sp_component_update_status(sp_type, sp_slot, component)
                    .await?;

                debug!(
                    log,
                    "got update status";
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
    /// the update was completed successfully
    Completed,
    /// the update could not be completed because it assumed a precondition that
    /// wasn't true (e.g., the device was currently running from a different
    /// slot than expected)
    PreconditionFailed, // XXX-dap more details
    /// the update we tried to complete was aborted (possibly by a different
    /// Nexus instance)
    Aborted(Uuid),
    /// the SP reports that our update failed
    Failed(Uuid, String),
    /// the SP reports that our attempt to reset the component failed
    ResetFailed(String),
    /// we gave up during the "updating" phase because it stopped making
    /// progress
    StuckUpdating(Uuid), // XXX-dap more details
    /// the SP unexpectedly behaved as though our update had never started
    Lost,
}

#[derive(Debug, Error)]
pub enum ApplyUpdateError {
    #[error("found no MGS backends in DNS")]
    NoMgsBackends,
    #[error("failed to fetch artifact")]
    FetchArtifact(#[from] ArtifactCacheError),
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

/// Makes one complete attempt to apply the specified software update to an SP
/// component.
///
/// `sp_update` specifies the target component, expected previous version,
/// expected new version, etc.
///
/// This operation is made up of multiple steps that have to happen in order:
/// upload the artifact to MGS, wait for the SP to finish writing it,
/// potentially change the active slot, reset the device, etc.  At any point,
/// it's possible that a different Nexus instance decides we're out to lunch and
/// aborts the process and potentially even starts its own.  While this function
/// will poll forever as long as it looks like things are working, it will
/// return early if not (e.g., if we find that somebody else has aborted our
/// update or if there's been no progress for too long).
///
/// // XXX-dap should these various failures be part of the Error enum instead?
/// The final result, when known, is reported as an [`ApplyUpdateResult`].
/// [`ApplyUpdateError`] is used for failures to even _try_.
/// XXX-dap we should be able to say that the final state is that no update is
/// in progress?
pub async fn apply_update(
    artifacts: Arc<ArtifactCache>,
    sp_update: &SpComponentUpdate,
    updater: &(dyn ReconfiguratorSpComponentUpdater + Send + Sync),
    mgs_rx: watch::Receiver<AllBackends>,
    update: &PendingMgsUpdate,
) -> Result<ApplyUpdateResult, ApplyUpdateError> {
    // Set up clients to talk to MGS.
    // XXX-dap rather than this, MgsClients probably ought to have a mode where
    // it accepts a qorb pool and continually try all clients forever on
    // transient issues.
    let log = &sp_update.log;
    let mut mgs_clients = {
        let backends = mgs_rx.borrow();
        if backends.is_empty() {
            return Err(ApplyUpdateError::NoMgsBackends);
        }
        MgsClients::from_clients(backends.iter().map(
            |(backend_name, backend)| {
                gateway_client::Client::new(
                    &format!("http://{}", backend.address),
                    log.new(o!(
                        "mgs_backend_name" => backend_name.0.to_string(),
                        "mgs_backend_addr" => backend.address.to_string(),
                    )),
                )
            },
        ))

        // It's important that `backends` is dropped at this point.  Otherwise,
        // we'll hold the watch channel lock while we do the long-running
        // operations below.
    };

    // Obtain the contents of the artifact that we need.
    let data =
        artifacts.artifact_contents(&update.artifact_hash_id.hash).await?;

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
    match updater.version_status(&mut mgs_clients, update).await? {
        VersionStatus::UpdateComplete => {
            return Ok(ApplyUpdateResult::Completed);
        }
        VersionStatus::NotReadyForUpdate => {
            return Ok(ApplyUpdateResult::PreconditionFailed);
        }
        VersionStatus::ReadyForUpdate => (),
    };

    // Start the update.
    let log = &sp_update.log;
    let sp_type = sp_update.target_sp_type;
    let sp_slot = sp_update.target_sp_slot;
    let component = sp_update.component();
    let my_update_id = sp_update.update_id;

    let update_start = mgs_clients
        .try_all_serially(log, |client| {
            let data = data.clone();
            async move {
                client
                    .sp_component_update(
                        sp_type,
                        sp_slot,
                        component,
                        sp_update.firmware_slot,
                        &sp_update.update_id,
                        reqwest::Body::from(data.clone()),
                    )
                    .await?;
                info!(
                    log,
                    "update started";
                    "mgs_addr" => client.baseurl(),
                );
                Ok(())
            }
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

    let our_update = match wait_for_delivery(&mut mgs_clients, sp_update)
        .await?
    {
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
            &mut mgs_clients,
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
        while let Err(error) =
            updater.post_update(&mut mgs_clients, update).await
        {
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
    match wait_for_update_done(updater, &mut mgs_clients, update, None).await {
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

    // XXX-dap something needs to abort after failure cases
}

/// Errors returned from `wait_for_update_done()`.
#[derive(Debug, Error)]
enum UpdateWaitError {
    #[error("timed out after {0:?}")]
    Timeout(Duration),
    #[error("found to be running neither previous version nor old version")]
    Indeterminate,
}

/// Waits for the specified update to finish (by polling)
///
/// This is called after the caller has determined than a particular update
/// (from one specific version to another) is ongoing, potentially driven by a
/// different Nexus instance.
///
/// Returns early with an error if it's determined that the target component is
/// running neither the old software (prior to the update) nor the new one
/// (after the update).
async fn wait_for_update_done(
    updater: &(dyn ReconfiguratorSpComponentUpdater + Send + Sync),
    mgs_clients: &mut MgsClients,
    update: &PendingMgsUpdate,
    timeout: Option<Duration>,
) -> Result<(), UpdateWaitError> {
    let before = Instant::now();

    // We retry this until we get some error *other* than a communication
    // error or the caller wants to give up due to a timeout.

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
