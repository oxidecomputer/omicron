// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Concurrent-safe facilities for doing MGS-managed upates

use crate::common_sp_update::PrecheckError;
use crate::common_sp_update::PrecheckStatus;
use crate::common_sp_update::STATUS_POLL_INTERVAL;
use crate::common_sp_update::SpComponentUpdateHelper;
use crate::driver::UpdateAttemptStatus;
use crate::driver::UpdateAttemptStatusUpdater;
use crate::mgs_clients::GatewayClientError;
use crate::{ArtifactCache, ArtifactCacheError, MgsClients};
use gateway_client::SpComponent;
use gateway_client::types::UpdateAbortBody;
use gateway_client::types::{SpType, SpUpdateStatus};
use nexus_types::deployment::PendingMgsUpdate;
use qorb::resolver::AllBackends;
use slog::{debug, error, info, o, warn};
use slog_error_chain::InlineErrorChain;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use thiserror::Error;
use tokio::sync::watch;
use uuid::Uuid;

/// How long may the status remain unchanged without us treating this as a
/// problem?
const PROGRESS_TIMEOUT: Duration = Duration::from_secs(120);

/// How long to wait between failed attempts to reset the device
const RESET_DELAY_INTERVAL: Duration = Duration::from_secs(10);

/// How long to wait between poll attempts on update status
const PROGRESS_POLL_INTERVAL: Duration = Duration::from_secs(10);

/// Timeout for repeat attempts
pub const DEFAULT_RETRY_TIMEOUT: Duration = Duration::from_secs(60);

/// How long to wait after resetting the device before expecting it to come up
const RESET_TIMEOUT: Duration = Duration::from_secs(60);

/// Parameters describing a request to update one SP-managed component
///
/// This is similar in spirit to the `SpComponentUpdater` trait but uses a
/// struct-based interface instead.
pub struct SpComponentUpdate {
    pub log: slog::Logger,
    pub component: SpComponent,
    pub target_sp_type: SpType,
    pub target_sp_slot: u32,
    pub firmware_slot: u16,
    pub update_id: Uuid,
}

impl SpComponentUpdate {
    fn component(&self) -> &str {
        self.component.const_as_str()
    }
}

#[derive(Clone, Debug)]
pub enum UpdateCompletedHow {
    FoundNoChangesNeeded,
    CompletedUpdate,
    WaitedForConcurrentUpdate,
    TookOverConcurrentUpdate,
}

#[derive(Debug, Error)]
pub enum ApplyUpdateError {
    #[error("found no MGS backends in DNS")]
    NoMgsBackends,
    #[error("failed to fetch artifact")]
    FetchArtifact(#[from] ArtifactCacheError),
    #[error("preconditions were not met")]
    PreconditionFailed(#[source] PrecheckError),
    #[error("SP reports update {0} was aborted")]
    SpUpdateAborted(Uuid),
    #[error("SP reports update {0} failed: {1:?}")]
    SpUpdateFailed(Uuid, String),
    #[error("SP not knowing about our update attempt")]
    SpUpdateLost,
    #[error(
        "gave up after {}ms waiting for update {0} to finish",
        .1.as_millis())
    ]
    StuckUpdating(Uuid, Duration),
    #[error("failed to abort in-progress SP update")]
    SpUpdateAbortFailed(#[from] AbortError),
    #[error("SP reports that reset failed: {0:?}")]
    SpResetFailed(String),

    #[error("failed waiting for artifact delivery")]
    DeliveryWaitError(#[from] DeliveryWaitError),
    #[error("error communicating with MGS")]
    UpdateStartError(#[source] GatewayClientError),
    #[error(
        "timed out after {}ms waiting for update to finish",
        .0.as_millis()
    )]
    ResetTimeoutError(Duration),
    #[error("waiting for update to finish")]
    WaitError(#[source] PrecheckError),
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
/// On success, `UpdateCompletedHow` describes what was needed to complete the
/// update.
pub(crate) async fn apply_update(
    artifacts: Arc<ArtifactCache>,
    sp_update: &SpComponentUpdate,
    update_helper: &(dyn SpComponentUpdateHelper + Send + Sync),
    mgs_rx: watch::Receiver<AllBackends>,
    update: &PendingMgsUpdate,
    status: UpdateAttemptStatusUpdater,
) -> Result<UpdateCompletedHow, ApplyUpdateError> {
    // Set up an instance of `MgsClients` to talk to MGS for the duration of
    // this attempt.  For each call to `try_serially()`, `MgsClients` will try
    // the request against each MGS client that it has.  That makes it possible
    // to survive transient failure of MGS as long as one is working.  If all
    // are offline, these operations will fail.  We rely on the higher-level
    // operation retry to deal with that.
    status.update(UpdateAttemptStatus::FetchingArtifact);
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
    debug!(log, "loaded artifact contents");

    // Check the live state first to see if:
    // - this update has already been completed, or
    // - if not, then if our required preconditions are met
    status.update(UpdateAttemptStatus::Precheck);
    match update_helper.precheck(log, &mut mgs_clients, update).await {
        Ok(PrecheckStatus::ReadyForUpdate) => (),
        Ok(PrecheckStatus::UpdateComplete) => {
            return Ok(UpdateCompletedHow::FoundNoChangesNeeded);
        }
        Err(error) => {
            return Err(ApplyUpdateError::PreconditionFailed(error));
        }
    };

    // Start the update.
    debug!(log, "ready to start update");
    status.update(UpdateAttemptStatus::Updating);
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
        Ok(()) => {
            debug!(log, "started update");
        }
        Err(error) => {
            // TODO We need a better way to identify this error.
            let chain = InlineErrorChain::new(&error);
            let message = chain.to_string();
            if !message.contains("update still in progress") {
                error!(log, "failed to start update"; chain);
                return Err(ApplyUpdateError::UpdateStartError(error));
            }

            // There's another one ongoing.  That's fine.
            // We'll handle this below.
            debug!(log, "watching existing update");
        }
    };

    status.update(UpdateAttemptStatus::UpdateWaiting);
    let our_update =
        match wait_for_delivery(&mut mgs_clients, sp_update).await? {
            DeliveryWaitStatus::Completed(id) => id == my_update_id,
            DeliveryWaitStatus::Aborted(id) => {
                warn!(
                    log,
                    "SP reports update was aborted";
                    "aborted_update_id" => id.to_string()
                );
                return Err(ApplyUpdateError::SpUpdateAborted(id));
            }
            DeliveryWaitStatus::NotRunning => {
                // This is a little weird.  The SP has likely been reset.
                warn!(log, "SP unexpectedly reports no update in progress");
                return Err(ApplyUpdateError::SpUpdateLost);
            }

            // For any of the following cases: something went wrong with the
            // update.  It needs to be explicitly aborted before anybody can try
            // again.  We'll attempt the abort and then report the specific
            // error.  The caller will have to do the retry if they want it.
            DeliveryWaitStatus::StuckUpdating(id, timeout) => {
                abort_update(&mut mgs_clients, sp_update, id, "stuck").await?;
                return Err(ApplyUpdateError::StuckUpdating(id, timeout));
            }

            DeliveryWaitStatus::Failed(id, message) => {
                abort_update(&mut mgs_clients, sp_update, id, "failed").await?;
                return Err(ApplyUpdateError::SpUpdateFailed(id, message));
            }
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
            log,
            update_helper,
            &mut mgs_clients,
            update,
            PROGRESS_TIMEOUT,
        )
        .await
        {
            Ok(_) => false,
            Err(UpdateWaitError::Timeout(_)) => {
                warn!(log, "update takeover: sending reset");
                true
            }
            Err(UpdateWaitError::Indeterminate(error)) => {
                return Err(ApplyUpdateError::WaitError(error));
            }
        }
    };

    debug!(log, "delivered artifact");
    status.update(UpdateAttemptStatus::PostUpdate);

    if try_reset {
        // We retry this until we get some error *other* than a communication
        // error.  There is intentionally no timeout here.  If we've staged an
        // update but not managed to reset the device, there's no point where
        // we'd want to stop trying to do so.
        while let Err(error) =
            update_helper.post_update(log, &mut mgs_clients, update).await
        {
            if !matches!(error, gateway_client::Error::CommunicationError(_)) {
                let error = InlineErrorChain::new(&error);
                error!(log, "post_update failed"; &error);
                return Err(ApplyUpdateError::SpResetFailed(error.to_string()));
            }

            tokio::time::sleep(RESET_DELAY_INTERVAL).await;
        }
    }

    // Regardless of whether it was our job to reset the device, wait for it to
    // come back on the new version.
    status.update(UpdateAttemptStatus::PostUpdateWait);
    let rv = match wait_for_update_done(
        log,
        update_helper,
        &mut mgs_clients,
        update,
        RESET_TIMEOUT,
    )
    .await
    {
        Ok(()) => {
            let how = match (our_update, try_reset) {
                (true, _) => UpdateCompletedHow::CompletedUpdate,
                (false, false) => UpdateCompletedHow::WaitedForConcurrentUpdate,
                (false, true) => UpdateCompletedHow::TookOverConcurrentUpdate,
            };
            Ok(how)
        }
        Err(UpdateWaitError::Timeout(error)) => {
            Err(ApplyUpdateError::ResetTimeoutError(error))
        }
        Err(UpdateWaitError::Indeterminate(error)) => {
            Err(ApplyUpdateError::WaitError(error))
        }
    };

    status.update(UpdateAttemptStatus::Done);
    rv
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
    StuckUpdating(Uuid, Duration),
}

#[derive(Debug, Error)]
pub enum DeliveryWaitError {
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
///
/// This returns an error only when the state is indeterminate.  Otherwise, it
/// returns a description of the resting state.
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
                                "timeout_ms" => PROGRESS_TIMEOUT.as_millis(),
                            );

                            return Ok(DeliveryWaitStatus::StuckUpdating(
                                id,
                                PROGRESS_TIMEOUT,
                            ));
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

#[derive(Debug, Error)]
#[error(
    "error aborting update {update_id} (reason: {reason}): error \
     communicating with MGS"
)]
pub struct AbortError {
    update_id: Uuid,
    reason: String,
    #[source]
    error: GatewayClientError,
}

async fn abort_update(
    mgs_clients: &mut MgsClients,
    sp_update: &SpComponentUpdate,
    update_id: Uuid,
    reason: &str,
) -> Result<(), AbortError> {
    let log = &sp_update.log;
    let sp_type = sp_update.target_sp_type;
    let sp_slot = sp_update.target_sp_slot;
    let component = sp_update.component();

    warn!(
        log,
        "aborting in-progress SP component update";
        "update_id" => update_id.to_string(),
        "reason" => reason,
    );

    mgs_clients
        .try_all_serially(log, |mgs_client| async move {
            let arg = UpdateAbortBody { id: update_id };
            mgs_client
                .sp_component_update_abort(sp_type, sp_slot, component, &arg)
                .await
        })
        .await
        .map_err(|error| AbortError {
            update_id,
            reason: reason.to_string(),
            error,
        })?;
    Ok(())
}

/// Errors returned from `wait_for_update_done()`.
#[derive(Debug, Error)]
enum UpdateWaitError {
    #[error("timed out after {0:?}")]
    Timeout(Duration),
    #[error("found unexpected state while waiting for update")]
    Indeterminate(#[source] PrecheckError),
}

/// Waits for the specified update to completely finish (by polling)
///
/// "Finish" here means that the component is online in the final state
/// reflected by the update (e.g., with the expected software in the active
/// slot).  If a reset was required as part of the update, then the component
/// will have come back online on the new software.
///
/// This is called after the caller has determined than a particular update
/// (from one specific version to another) is ongoing, potentially driven by a
/// different Nexus instance.
///
/// Returns early with an error if it's determined that the target component is
/// running neither the old software (prior to the update) nor the new one
/// (after the update).
async fn wait_for_update_done(
    log: &slog::Logger,
    updater: &(dyn SpComponentUpdateHelper + Send + Sync),
    mgs_clients: &mut MgsClients,
    update: &PendingMgsUpdate,
    timeout: Duration,
) -> Result<(), UpdateWaitError> {
    let before = Instant::now();

    // We retry this until we get some error *other* than a communication
    // error or the caller wants to give up due to a timeout.

    loop {
        let precheck = updater.precheck(log, mgs_clients, update).await;
        debug!(log, "precheck result"; "precheck" => ?precheck);
        match updater.precheck(log, mgs_clients, update).await {
            // Check if we're done.
            Ok(PrecheckStatus::UpdateComplete) => return Ok(()),

            // An incorrect version in the "inactive" slot is normal during the
            // upgrade.  We have no reason to think this won't converge so we
            // proceed with waiting.
            Err(PrecheckError::GatewayClientError(_))
            | Err(PrecheckError::WrongInactiveVersion { .. })
            | Ok(PrecheckStatus::ReadyForUpdate) => {
                if before.elapsed() >= timeout {
                    return Err(UpdateWaitError::Timeout(timeout));
                }

                tokio::time::sleep(PROGRESS_POLL_INTERVAL).await;
                continue;
            }

            Err(error @ PrecheckError::WrongDevice { .. })
            | Err(error @ PrecheckError::WrongActiveVersion { .. }) => {
                // Stop trying to make this update happen.  It's not going to
                // happen.
                return Err(UpdateWaitError::Indeterminate(error));
            }
        }
    }
}
