// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Concurrent-safe facilities for doing MGS-managed upates

use crate::common_sp_update::PrecheckError;
use crate::common_sp_update::PrecheckStatus;
use crate::common_sp_update::STATUS_POLL_INTERVAL;
use crate::common_sp_update::SpComponentUpdateHelper;
use crate::driver::UpdateAttemptStatusUpdater;
use crate::mgs_clients::GatewayClientError;
use crate::{ArtifactCache, ArtifactCacheError, MgsClients};
use gateway_client::SpComponent;
use gateway_client::types::UpdateAbortBody;
use gateway_client::types::{SpType, SpUpdateStatus};
use nexus_types::deployment::PendingMgsUpdate;
use nexus_types::deployment::PendingMgsUpdateDetails;
use nexus_types::internal_api::views::UpdateAttemptStatus;
use nexus_types::internal_api::views::UpdateCompletedHow;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SpUpdateUuid;
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
//
// Generally, this value covers two different things:
//
// 1. While we're uploading an image to the SP or it's being prepared, how long
//     can the status stay the same before we give up altogether and try again?
//     In practice, this would rarely pause for more than a few seconds.
// 2. The period where we might wait for an update to complete -- either our own
//     update (in which case this is the period after the final device reset
//     until the device comes up reporting the new version) or another instance's
//     update (in which case this could cover almost the _entire_ update
//     process).
//
// In both cases, if the timeout is reached, the whole update attempt will fail.
// This behavior is only intended to deal with pathological cases, like an MGS
// crash (which could cause an upload to hang indefinitely) or a Nexus crash
// (which could cause any update to hang indefinitely at any point). So we can
// afford to be generous here. Further, we really don't want to trip this
// erroneously in a working system because we're likely to get stuck continuing
// to retry and give up before each attempt finishes.
//
// In terms of sizing this timeout:
// - For all updates, the upload phase generally takes 10-20 seconds.
// - For SP updates, the post-reset phase can take about 30s (with Sidecar SPs
//    being the longest).
// - For RoT and RoT bootloader updates, two resets and an intervening "set
//    active slot" operation are required. Together, these could take just a
//    few seconds.
//
// Adding all the above together, and giving ourselves plenty of margin, we
// choose 10 minutes.
pub const PROGRESS_TIMEOUT: Duration = Duration::from_secs(600);

/// How long to wait between failed attempts to reset the device
const RESET_DELAY_INTERVAL: Duration = Duration::from_secs(10);

/// How long to wait between poll attempts on update status
const PROGRESS_POLL_INTERVAL: Duration = Duration::from_secs(10);

/// Timeout for repeat attempts
pub const DEFAULT_RETRY_TIMEOUT: Duration = Duration::from_secs(60);

/// How long to wait after resetting the device before expecting it to come up
// 120 seconds is chosen as a generous overestimate, based on reports that
// Sidecar SPs have been observed to take as many as 30 seconds to reset.
const RESET_TIMEOUT: Duration = Duration::from_secs(120);

/// Parameters describing a request to update one SP-managed component
///
/// This is similar in spirit to the `SpComponentUpdater` trait but uses a
/// struct-based interface instead.
pub(crate) struct SpComponentUpdate {
    pub log: slog::Logger,
    pub component: SpComponent,
    pub target_sp_type: SpType,
    pub target_sp_slot: u16,
    pub firmware_slot: u16,
    pub update_id: SpUpdateUuid,
}

impl SpComponentUpdate {
    fn component(&self) -> &str {
        self.component.const_as_str()
    }

    pub fn from_request(
        log: &slog::Logger,
        request: &PendingMgsUpdate,
        update_id: SpUpdateUuid,
    ) -> SpComponentUpdate {
        match &request.details {
            PendingMgsUpdateDetails::Sp { .. } => SpComponentUpdate {
                log: log.clone(),
                component: SpComponent::SP_ITSELF,
                target_sp_type: request.sp_type,
                target_sp_slot: request.slot_id,
                // The SP has two firmware slots, but they're aren't
                // individually labeled. We always request an update to slot
                // 0, which (confusingly in this context) means "the
                // inactive slot".
                firmware_slot: 0,
                update_id,
            },
            PendingMgsUpdateDetails::Rot { expected_active_slot, .. } => {
                SpComponentUpdate {
                    log: log.clone(),
                    component: SpComponent::ROT,
                    target_sp_type: request.sp_type,
                    target_sp_slot: request.slot_id,
                    // Like the SP, we request an update to the inactive slot
                    firmware_slot: expected_active_slot
                        .slot()
                        .toggled()
                        .to_u16(),
                    update_id,
                }
            }
            PendingMgsUpdateDetails::RotBootloader { .. } => {
                SpComponentUpdate {
                    log: log.clone(),
                    component: SpComponent::STAGE0,
                    target_sp_type: request.sp_type,
                    target_sp_slot: request.slot_id,
                    // The RoT bootloader has two firmware slots, stage0 and
                    // stage0next. We always request an update to stage0next, which
                    // is the staging area for the bootloader and in this context
                    // means "the inactive slot".
                    firmware_slot: 1,
                    update_id,
                }
            }
        }
    }
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
    #[error("SP reports not knowing about our update attempt")]
    SpUpdateLost,
    #[error(
        "gave up after {}ms waiting for update {update_id} to finish",
        timeout.as_millis())
    ]
    StuckUpdating { update_id: Uuid, timeout: Duration },
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
    progress_timeout: Duration,
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
    let data = artifacts.artifact_contents(&update.artifact_hash).await?;
    debug!(log, "loaded artifact contents");

    // Check the live state first to see if:
    // - this update has already been completed,
    // - we should wait a bit because an update may be in-progress, or
    // - if not, then if our required preconditions are met
    status.update(UpdateAttemptStatus::Precheck);
    let before = Instant::now();
    loop {
        match update_helper.precheck(log, &mut mgs_clients, update).await {
            Ok(PrecheckStatus::ReadyForUpdate) => break,
            Ok(PrecheckStatus::WaitingForOngoingUpdate) => {
                if before.elapsed() >= progress_timeout {
                    warn!(
                        log,
                        "update takeover: timed out while waiting for ongoing update"
                    );
                    break;
                }

                tokio::time::sleep(PROGRESS_POLL_INTERVAL).await;
                continue;
            }
            Ok(PrecheckStatus::UpdateComplete) => {
                return Ok(UpdateCompletedHow::FoundNoChangesNeeded);
            }
            Err(error) => {
                return Err(ApplyUpdateError::PreconditionFailed(error));
            }
        };
    }
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
                        &sp_update.update_id.as_untyped_uuid(),
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
            DeliveryWaitStatus::Completed(id) => {
                id == my_update_id.into_untyped_uuid()
            }
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
                return Err(ApplyUpdateError::StuckUpdating {
                    update_id: id,
                    timeout,
                });
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
            progress_timeout,
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
        // error or some other transient error.  There is intentionally no
        // timeout here.  If we've staged an update but not managed to reset
        // the device, there's no point where we'd want to stop trying to do so.
        while let Err(error) =
            update_helper.post_update(log, &mut mgs_clients, update).await
        {
            if error.is_fatal() {
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

            // An incorrect version in the "inactive" slot, incorrect active slot,
            // or non-empty pending_persistent_boot_preference/transient_boot_preference
            // are normal during the upgrade. We have no reason to think these won't
            // converge so we proceed with waiting.
            Err(PrecheckError::GatewayClientError(_))
            | Err(PrecheckError::WrongInactiveVersion { .. })
            | Err(PrecheckError::WrongActiveSlot { .. })
            | Err(PrecheckError::EphemeralRotBootPreferenceSet)
            | Ok(PrecheckStatus::WaitingForOngoingUpdate)
            | Ok(PrecheckStatus::ReadyForUpdate) => {
                if before.elapsed() >= timeout {
                    return Err(UpdateWaitError::Timeout(timeout));
                }

                tokio::time::sleep(PROGRESS_POLL_INTERVAL).await;
                continue;
            }

            Err(error @ PrecheckError::WrongDevice { .. })
            | Err(error @ PrecheckError::WrongActiveVersion { .. })
            | Err(error @ PrecheckError::RotCommunicationFailed { .. }) => {
                // Stop trying to make this update happen.  It's not going to
                // happen.
                return Err(UpdateWaitError::Indeterminate(error));
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::ApplyUpdateError;
    use crate::test_util::test_artifacts::TestArtifacts;
    use crate::test_util::updates::ExpectedSpComponent;
    use crate::test_util::updates::UpdateDescription;
    use assert_matches::assert_matches;
    use gateway_client::types::SpType;
    use gateway_messages::SpPort;
    use gateway_test_utils::setup::GatewayTestContext;
    use gateway_types::rot::RotSlot;
    use nexus_types::deployment::ExpectedActiveRotSlot;
    use nexus_types::deployment::ExpectedVersion;
    use nexus_types::internal_api::views::UpdateAttemptStatus;
    use nexus_types::internal_api::views::UpdateCompletedHow;
    use nexus_types::inventory::BaseboardId;
    use slog_error_chain::InlineErrorChain;
    use std::time::Duration;
    use tufaceous_artifact::ArtifactHash;

    /// Tests several happy-path cases of updating an SP
    #[tokio::test]
    async fn test_sp_update_basic() {
        let gwtestctx = gateway_test_utils::setup::test_setup(
            "test_sp_update_basic",
            SpPort::One,
        )
        .await;
        let log = &gwtestctx.logctx.log;
        let artifacts = TestArtifacts::new(log).await.unwrap();

        // Basic case: normal update
        run_one_successful_sp_update(
            &gwtestctx,
            &artifacts,
            SpType::Sled,
            1,
            &artifacts.sp_gimlet_artifact_hash,
            UpdateCompletedHow::CompletedUpdate,
        )
        .await;

        // Basic case: attempted update, found no changes needed
        run_one_successful_sp_update(
            &gwtestctx,
            &artifacts,
            SpType::Sled,
            1,
            &artifacts.sp_gimlet_artifact_hash,
            UpdateCompletedHow::FoundNoChangesNeeded,
        )
        .await;

        // Run the same two tests for a switch SP.
        run_one_successful_sp_update(
            &gwtestctx,
            &artifacts,
            SpType::Switch,
            0,
            &artifacts.sp_sidecar_artifact_hash,
            UpdateCompletedHow::CompletedUpdate,
        )
        .await;
        run_one_successful_sp_update(
            &gwtestctx,
            &artifacts,
            SpType::Switch,
            0,
            &artifacts.sp_sidecar_artifact_hash,
            UpdateCompletedHow::FoundNoChangesNeeded,
        )
        .await;

        artifacts.teardown().await;
        gwtestctx.teardown().await;
    }

    async fn run_one_successful_sp_update(
        gwtestctx: &GatewayTestContext,
        artifacts: &TestArtifacts,
        sp_type: SpType,
        slot_id: u16,
        artifact_hash: &ArtifactHash,
        expected_result: UpdateCompletedHow,
    ) {
        let desc = UpdateDescription {
            gwtestctx,
            artifacts,
            sp_type,
            slot_id,
            artifact_hash,
            override_baseboard_id: None,
            override_expected_sp_component: ExpectedSpComponent::Sp {
                override_expected_active: None,
                override_expected_inactive: None,
            },
            override_progress_timeout: None,
        };

        let in_progress = desc.setup().await;
        let finished = in_progress.finish().await;
        finished.expect_sp_success(expected_result);
    }

    /// Tests several happy-path cases of updating an RoT bootloader
    #[tokio::test]
    async fn test_rot_bootloader_update_basic() {
        let gwtestctx = gateway_test_utils::setup::test_setup(
            "test_rot_bootloader_update_basic",
            SpPort::One,
        )
        .await;
        let log = &gwtestctx.logctx.log;
        let artifacts = TestArtifacts::new(log).await.unwrap();

        // Basic case: normal update
        run_one_successful_rot_bootloader_update(
            &gwtestctx,
            &artifacts,
            SpType::Sled,
            1,
            &artifacts.rot_bootloader_gimlet_artifact_hash,
            UpdateCompletedHow::CompletedUpdate,
        )
        .await;

        // Basic case: attempted update, found no changes needed
        run_one_successful_rot_bootloader_update(
            &gwtestctx,
            &artifacts,
            SpType::Sled,
            1,
            &artifacts.rot_bootloader_gimlet_artifact_hash,
            UpdateCompletedHow::FoundNoChangesNeeded,
        )
        .await;

        // Run the same two tests for a switch RoT bootloader.
        run_one_successful_rot_bootloader_update(
            &gwtestctx,
            &artifacts,
            SpType::Switch,
            0,
            &artifacts.rot_bootloader_sidecar_artifact_hash,
            UpdateCompletedHow::CompletedUpdate,
        )
        .await;
        run_one_successful_rot_bootloader_update(
            &gwtestctx,
            &artifacts,
            SpType::Switch,
            0,
            &artifacts.rot_bootloader_sidecar_artifact_hash,
            UpdateCompletedHow::FoundNoChangesNeeded,
        )
        .await;

        artifacts.teardown().await;
        gwtestctx.teardown().await;
    }

    async fn run_one_successful_rot_bootloader_update(
        gwtestctx: &GatewayTestContext,
        artifacts: &TestArtifacts,
        sp_type: SpType,
        slot_id: u16,
        artifact_hash: &ArtifactHash,
        expected_result: UpdateCompletedHow,
    ) {
        let desc = UpdateDescription {
            gwtestctx,
            artifacts,
            sp_type,
            slot_id,
            artifact_hash,
            override_baseboard_id: None,
            override_expected_sp_component:
                ExpectedSpComponent::RotBootloader {
                    override_expected_stage0: None,
                    override_expected_stage0_next: None,
                },
            override_progress_timeout: None,
        };

        let in_progress = desc.setup().await;
        let finished = in_progress.finish().await;
        finished.expect_rot_bootloader_success(expected_result);
    }

    /// Tests several happy-path cases of updating an RoT
    #[tokio::test]
    async fn test_rot_update_basic() {
        let gwtestctx = gateway_test_utils::setup::test_setup(
            "test_rot_update_basic",
            SpPort::One,
        )
        .await;
        let log = &gwtestctx.logctx.log;
        let artifacts = TestArtifacts::new(log).await.unwrap();

        // Basic case: normal update
        run_one_successful_rot_update(
            &gwtestctx,
            &artifacts,
            SpType::Sled,
            1,
            &artifacts.rot_gimlet_artifact_hash,
            UpdateCompletedHow::CompletedUpdate,
        )
        .await;

        // Basic case: attempted update, found no changes needed
        run_one_successful_rot_update(
            &gwtestctx,
            &artifacts,
            SpType::Sled,
            1,
            &artifacts.rot_gimlet_artifact_hash,
            UpdateCompletedHow::FoundNoChangesNeeded,
        )
        .await;

        // Run the same two tests for a switch RoT.
        run_one_successful_rot_update(
            &gwtestctx,
            &artifacts,
            SpType::Switch,
            0,
            &artifacts.rot_sidecar_artifact_hash,
            UpdateCompletedHow::CompletedUpdate,
        )
        .await;
        run_one_successful_rot_update(
            &gwtestctx,
            &artifacts,
            SpType::Switch,
            0,
            &artifacts.rot_sidecar_artifact_hash,
            UpdateCompletedHow::FoundNoChangesNeeded,
        )
        .await;

        artifacts.teardown().await;
        gwtestctx.teardown().await;
    }

    async fn run_one_successful_rot_update(
        gwtestctx: &GatewayTestContext,
        artifacts: &TestArtifacts,
        sp_type: SpType,
        slot_id: u16,
        artifact_hash: &ArtifactHash,
        expected_result: UpdateCompletedHow,
    ) {
        let desc = UpdateDescription {
            gwtestctx,
            artifacts,
            sp_type,
            slot_id,
            artifact_hash,
            override_baseboard_id: None,
            override_expected_sp_component: ExpectedSpComponent::Rot {
                override_expected_active_slot: None,
                override_expected_inactive_version: None,
                override_expected_persistent_boot_preference: None,
                override_expected_pending_persistent_boot_preference: None,
                override_expected_transient_boot_preference: None,
            },
            override_progress_timeout: None,
        };

        let in_progress = desc.setup().await;
        let finished = in_progress.finish().await;
        finished.expect_rot_success(expected_result);
    }

    /// Tests the case where two updates run concurrently.  One notices another
    /// is running and waits for it to complete.
    #[tokio::test]
    async fn test_sp_update_watched() {
        let gwtestctx = gateway_test_utils::setup::test_setup(
            "test_sp_update_watched",
            SpPort::One,
        )
        .await;
        let log = &gwtestctx.logctx.log;
        let artifacts = TestArtifacts::new(log).await.unwrap();

        // We're going to start two concurrent update attempts.  The sequence
        // we want is:
        //
        // - update1 gets far enough along to have started the upload, but does
        //   not actually finish
        // - update2 gets far enough along to notice that update1 is running
        // - then update1 finished
        // - then update2 can finish
        //
        // It's a little tricky to orchestrate this with the tools we have
        // available.  The most robust is actually have update2's precondition
        // reflect that update1 has finished its upload.
        let desc1 = UpdateDescription {
            gwtestctx: &gwtestctx,
            artifacts: &artifacts,
            sp_type: SpType::Sled,
            slot_id: 1,
            artifact_hash: &artifacts.sp_gimlet_artifact_hash,
            override_baseboard_id: None,
            override_expected_sp_component: ExpectedSpComponent::Sp {
                override_expected_active: None,
                override_expected_inactive: None,
            },
            override_progress_timeout: None,
        };

        let desc2 = UpdateDescription {
            gwtestctx: &gwtestctx,
            artifacts: &artifacts,
            sp_type: SpType::Sled,
            slot_id: 1,
            artifact_hash: &artifacts.sp_gimlet_artifact_hash,
            override_baseboard_id: None,
            override_expected_sp_component: ExpectedSpComponent::Sp {
                override_expected_active: None,
                override_expected_inactive: Some(ExpectedVersion::Version(
                    std::str::from_utf8(
                        artifacts
                            .deployed_caboose(
                                &artifacts.sp_gimlet_artifact_hash,
                            )
                            .expect("deployed caboose for generated artifact")
                            .version()
                            .expect("valid version"),
                    )
                    .expect("version is UTF-8")
                    .parse()
                    .expect("version is valid"),
                )),
            },
            override_progress_timeout: None,
        };

        let mut in_progress1 = desc1.setup().await;
        let mut in_progress2 = desc2.setup().await;

        // Start one, but pause it while waiting for the update to upload.
        in_progress1.run_until_status(UpdateAttemptStatus::UpdateWaiting).await;

        // Start the other.  Pause it at the point where it's also waiting for
        // the upload to finish.
        in_progress2.run_until_status(UpdateAttemptStatus::UpdateWaiting).await;

        // Finish the first update, then the second.
        let finished1 = in_progress1.finish().await;
        let finished2 = in_progress2.finish().await;

        // Both should succeed, but with different codes.
        finished1.expect_sp_success(UpdateCompletedHow::CompletedUpdate);
        finished2
            .expect_sp_success(UpdateCompletedHow::WaitedForConcurrentUpdate);

        artifacts.teardown().await;
        gwtestctx.teardown().await;
    }

    /// Tests the case where an update takes over from a previously-started one.
    #[tokio::test]
    async fn test_sp_update_takeover() {
        let gwtestctx = gateway_test_utils::setup::test_setup(
            "test_sp_update_takeover",
            SpPort::One,
        )
        .await;
        let log = &gwtestctx.logctx.log;
        let artifacts = TestArtifacts::new(log).await.unwrap();

        // See the notes in test_sp_update_watched().  We start the same way.
        let desc1 = UpdateDescription {
            gwtestctx: &gwtestctx,
            artifacts: &artifacts,
            sp_type: SpType::Sled,
            slot_id: 1,
            artifact_hash: &artifacts.sp_gimlet_artifact_hash,
            override_baseboard_id: None,
            override_expected_sp_component: ExpectedSpComponent::Sp {
                override_expected_active: None,
                override_expected_inactive: None,
            },
            override_progress_timeout: None,
        };

        let desc2 = UpdateDescription {
            gwtestctx: &gwtestctx,
            artifacts: &artifacts,
            sp_type: SpType::Sled,
            slot_id: 1,
            artifact_hash: &artifacts.sp_gimlet_artifact_hash,
            override_baseboard_id: None,
            override_expected_sp_component: ExpectedSpComponent::Sp {
                override_expected_active: None,
                override_expected_inactive: Some(ExpectedVersion::Version(
                    std::str::from_utf8(
                        artifacts
                            .deployed_caboose(
                                &artifacts.sp_gimlet_artifact_hash,
                            )
                            .expect("deployed caboose for generated artifact")
                            .version()
                            .expect("valid version"),
                    )
                    .expect("version is UTF-8")
                    .parse()
                    .expect("version is valid"),
                )),
            },
            // This timeout (10 seconds) seeks to balance being long enough to
            // be relevant without making the tests take too long.  (It's
            // assumed that 10 seconds here is not a huge deal because this is
            // mostly idle time and this test is unlikely to be the long pole.)
            override_progress_timeout: Some(Duration::from_secs(10)),
        };

        let mut in_progress1 = desc1.setup().await;
        let mut in_progress2 = desc2.setup().await;

        // Start one, but pause it while waiting for the update to upload.
        in_progress1.run_until_status(UpdateAttemptStatus::UpdateWaiting).await;

        // Start the other.  Pause it at the point where it's also waiting for
        // the upload to finish.
        in_progress2.run_until_status(UpdateAttemptStatus::UpdateWaiting).await;

        // This time, resume the second update first.  It will take over the
        // first one.
        let finished2 = in_progress2.finish().await;
        finished2
            .expect_sp_success(UpdateCompletedHow::TookOverConcurrentUpdate);

        // Now if we resume the first update, it will find that the SP has been
        // reset out from under it.  This is the closest thing we have to a test
        // where the SP gets reset during the update.  This does verify that if
        // the SP gets reset during this phase, the code detects that and
        // doesn't get stuck.  With more control over the simulated SP (e.g.,
        // the ability to pause the upload on the SP side), we could more
        // exhaustively test what happens if the SP gets reset at each step in
        // the update process.
        in_progress1.finish().await.expect_failure(&|error, _sp1, _sp2| {
            assert_matches!(error, ApplyUpdateError::SpUpdateLost);
        });

        artifacts.teardown().await;
        gwtestctx.teardown().await;
    }

    /// Tests a bunch of easy fast-failure cases.
    #[tokio::test]
    async fn test_sp_basic_failures() {
        let gwtestctx = gateway_test_utils::setup::test_setup(
            "test_sp_basic_failures",
            SpPort::One,
        )
        .await;
        let log = &gwtestctx.logctx.log;
        let artifacts = TestArtifacts::new(log).await.unwrap();

        // Test a case of mistaken identity (reported baseboard does not match
        // the one that we expect).
        let desc = UpdateDescription {
            gwtestctx: &gwtestctx,
            artifacts: &artifacts,
            sp_type: SpType::Sled,
            slot_id: 1,
            artifact_hash: &artifacts.sp_gimlet_artifact_hash,
            override_baseboard_id: Some(BaseboardId {
                part_number: String::from("i86pc"),
                serial_number: String::from("SimGimlet0"),
            }),
            override_expected_sp_component: ExpectedSpComponent::Sp {
                override_expected_active: None,
                override_expected_inactive: None,
            },
            override_progress_timeout: None,
        };

        desc.setup().await.finish().await.expect_failure(&|error, sp1, sp2| {
            assert_matches!(error, ApplyUpdateError::PreconditionFailed(..));
            let message = InlineErrorChain::new(error).to_string();
            eprintln!("{}", message);
            assert!(message.contains(
                "in sled slot 1, expected to find part \"i86pc\" serial \
                     \"SimGimlet0\", but found part \"i86pc\" serial \
                     \"SimGimlet01\"",
            ));

            // No changes should have been made in this case.
            assert_eq!(sp1, sp2);
        });

        // Test a case where the active version doesn't match what we expect.
        let desc = UpdateDescription {
            gwtestctx: &gwtestctx,
            artifacts: &artifacts,
            sp_type: SpType::Sled,
            slot_id: 1,
            artifact_hash: &artifacts.sp_gimlet_artifact_hash,
            override_baseboard_id: None,
            override_expected_sp_component: ExpectedSpComponent::Sp {
                override_expected_active: Some("not-right".parse().unwrap()),
                override_expected_inactive: None,
            },
            override_progress_timeout: None,
        };

        desc.setup().await.finish().await.expect_failure(&|error, sp1, sp2| {
            assert_matches!(error, ApplyUpdateError::PreconditionFailed(..));
            let message = InlineErrorChain::new(error).to_string();
            eprintln!("{}", message);
            assert!(message.contains(
                "expected to find active version \"not-right\", but \
                     found \"0.0.2\""
            ));

            // No changes should have been made in this case.
            assert_eq!(sp1, sp2);
        });

        // Test a case where the inactive version doesn't match what it should
        // (expected invalid, found something else).
        let desc = UpdateDescription {
            gwtestctx: &gwtestctx,
            artifacts: &artifacts,
            sp_type: SpType::Sled,
            slot_id: 1,
            artifact_hash: &artifacts.sp_gimlet_artifact_hash,
            override_baseboard_id: None,
            override_expected_sp_component: ExpectedSpComponent::Sp {
                override_expected_active: None,
                override_expected_inactive: Some(
                    ExpectedVersion::NoValidVersion,
                ),
            },
            override_progress_timeout: None,
        };

        desc.setup().await.finish().await.expect_failure(&|error, sp1, sp2| {
            assert_matches!(error, ApplyUpdateError::PreconditionFailed(..));
            let message = InlineErrorChain::new(error).to_string();
            eprintln!("{}", message);
            assert!(message.contains(
                "expected to find inactive version NoValidVersion, \
                     but found Version(\"0.0.1\")"
            ));

            // No changes should have been made in this case.
            assert_eq!(sp1, sp2);
        });

        // Now test a case where the inactive version doesn't match what it
        // should (expected a different valid version).
        let desc = UpdateDescription {
            gwtestctx: &gwtestctx,
            artifacts: &artifacts,
            sp_type: SpType::Sled,
            slot_id: 1,
            artifact_hash: &artifacts.sp_gimlet_artifact_hash,
            override_baseboard_id: None,
            override_expected_sp_component: ExpectedSpComponent::Sp {
                override_expected_active: None,
                override_expected_inactive: Some(ExpectedVersion::Version(
                    "something-else".parse().unwrap(),
                )),
            },
            override_progress_timeout: None,
        };

        desc.setup().await.finish().await.expect_failure(&|error, sp1, sp2| {
            assert_matches!(error, ApplyUpdateError::PreconditionFailed(..));
            let message = InlineErrorChain::new(error).to_string();
            eprintln!("{}", message);
            assert!(message.contains(
                "expected to find inactive version \
                     Version(ArtifactVersion(\"something-else\")), but found \
                     Version(\"0.0.1\")"
            ));

            // No changes should have been made in this case.
            assert_eq!(sp1, sp2);
        });

        // Test a case where we fail to fetch the artifact.  We simulate this by
        // tearing down our artifact server before the update starts.
        let desc = UpdateDescription {
            gwtestctx: &gwtestctx,
            artifacts: &artifacts,
            sp_type: SpType::Sled,
            slot_id: 1,
            artifact_hash: &artifacts.sp_gimlet_artifact_hash,
            override_baseboard_id: None,
            override_expected_sp_component: ExpectedSpComponent::Sp {
                override_expected_active: None,
                override_expected_inactive: None,
            },
            override_progress_timeout: None,
        };
        let in_progress = desc.setup().await;
        artifacts.teardown().await;
        in_progress.finish().await.expect_failure(&|error, sp1, sp2| {
            assert_matches!(error, ApplyUpdateError::FetchArtifact(..));
            // No changes should have been made in this case.
            assert_eq!(sp1, sp2);
        });

        gwtestctx.teardown().await;
    }

    /// Tests several RoT update easy fast-failure cases.
    #[tokio::test]
    async fn test_rot_basic_failures() {
        let gwtestctx = gateway_test_utils::setup::test_setup(
            "test_rot_basic_failures",
            SpPort::One,
        )
        .await;
        let log = &gwtestctx.logctx.log;
        let artifacts = TestArtifacts::new(log).await.unwrap();

        // Test a case of mistaken identity (reported baseboard does not match
        // the one that we expect).
        let desc = UpdateDescription {
            gwtestctx: &gwtestctx,
            artifacts: &artifacts,
            sp_type: SpType::Sled,
            slot_id: 1,
            artifact_hash: &artifacts.sp_gimlet_artifact_hash,
            override_baseboard_id: Some(BaseboardId {
                part_number: String::from("i86pc"),
                serial_number: String::from("SimGimlet0"),
            }),
            override_expected_sp_component: ExpectedSpComponent::Rot {
                override_expected_active_slot: None,
                override_expected_inactive_version: None,
                override_expected_persistent_boot_preference: None,
                override_expected_pending_persistent_boot_preference: None,
                override_expected_transient_boot_preference: None,
            },
            override_progress_timeout: None,
        };

        desc.setup().await.finish().await.expect_failure(&|error, sp1, sp2| {
            assert_matches!(error, ApplyUpdateError::PreconditionFailed(..));
            let message = InlineErrorChain::new(error).to_string();
            eprintln!("{}", message);
            assert!(message.contains(
                "in sled slot 1, expected to find part \"i86pc\" serial \
                 \"SimGimlet0\", but found part \"i86pc\" serial \
                 \"SimGimlet01\"",
            ));

            // No changes should have been made in this case.
            assert_eq!(sp1, sp2);
        });

        // Test a case where the active version doesn't match what we expect.
        let desc = UpdateDescription {
            gwtestctx: &gwtestctx,
            artifacts: &artifacts,
            sp_type: SpType::Sled,
            slot_id: 1,
            artifact_hash: &artifacts.sp_gimlet_artifact_hash,
            override_baseboard_id: None,
            override_expected_sp_component: ExpectedSpComponent::Rot {
                override_expected_active_slot: Some(ExpectedActiveRotSlot {
                    slot: RotSlot::A,
                    version: "not-right".parse().unwrap(),
                }),
                override_expected_inactive_version: None,
                override_expected_persistent_boot_preference: None,
                override_expected_pending_persistent_boot_preference: None,
                override_expected_transient_boot_preference: None,
            },
            override_progress_timeout: None,
        };

        desc.setup().await.finish().await.expect_failure(&|error, sp1, sp2| {
            assert_matches!(error, ApplyUpdateError::PreconditionFailed(..));
            let message = InlineErrorChain::new(error).to_string();
            eprintln!("{}", message);
            assert!(message.contains(
                "expected to find active version \"not-right\", but \
                     found \"0.0.4\""
            ));

            // No changes should have been made in this case.
            assert_eq!(sp1, sp2);
        });

        // Test a case where the active slot doesn't match what we expect.
        let desc = UpdateDescription {
            gwtestctx: &gwtestctx,
            artifacts: &artifacts,
            sp_type: SpType::Sled,
            slot_id: 1,
            artifact_hash: &artifacts.sp_gimlet_artifact_hash,
            override_baseboard_id: None,
            override_expected_sp_component: ExpectedSpComponent::Rot {
                override_expected_active_slot: Some(ExpectedActiveRotSlot {
                    slot: RotSlot::B,
                    version: "0.0.4".parse().unwrap(),
                }),
                override_expected_inactive_version: None,
                override_expected_persistent_boot_preference: None,
                override_expected_pending_persistent_boot_preference: None,
                override_expected_transient_boot_preference: None,
            },
            override_progress_timeout: None,
        };

        desc.setup().await.finish().await.expect_failure(&|error, sp1, sp2| {
            assert_matches!(error, ApplyUpdateError::PreconditionFailed(..));
            let message = InlineErrorChain::new(error).to_string();
            eprintln!("{}", message);
            assert!(
                message.contains("expected to find active slot B, but found A")
            );

            // No changes should have been made in this case.
            assert_eq!(sp1, sp2);
        });

        // Test a case where the inactive version doesn't match what it should
        // (expected invalid, found something else).
        let desc = UpdateDescription {
            gwtestctx: &gwtestctx,
            artifacts: &artifacts,
            sp_type: SpType::Sled,
            slot_id: 1,
            artifact_hash: &artifacts.sp_gimlet_artifact_hash,
            override_baseboard_id: None,
            override_expected_sp_component: ExpectedSpComponent::Rot {
                override_expected_active_slot: None,
                override_expected_inactive_version: Some(
                    ExpectedVersion::NoValidVersion,
                ),
                override_expected_persistent_boot_preference: None,
                override_expected_pending_persistent_boot_preference: None,
                override_expected_transient_boot_preference: None,
            },
            override_progress_timeout: None,
        };

        desc.setup().await.finish().await.expect_failure(&|error, sp1, sp2| {
            assert_matches!(error, ApplyUpdateError::PreconditionFailed(..));
            let message = InlineErrorChain::new(error).to_string();
            eprintln!("{}", message);
            assert!(message.contains(
                "expected to find inactive version NoValidVersion, \
                     but found Version(\"0.0.3\")"
            ));

            // No changes should have been made in this case.
            assert_eq!(sp1, sp2);
        });

        // Now test a case where the inactive version doesn't match what it
        // should (expected a different valid version).
        let desc = UpdateDescription {
            gwtestctx: &gwtestctx,
            artifacts: &artifacts,
            sp_type: SpType::Sled,
            slot_id: 1,
            artifact_hash: &artifacts.sp_gimlet_artifact_hash,
            override_baseboard_id: None,
            override_expected_sp_component: ExpectedSpComponent::Rot {
                override_expected_active_slot: None,
                override_expected_inactive_version: Some(
                    ExpectedVersion::Version("something-else".parse().unwrap()),
                ),
                override_expected_persistent_boot_preference: None,
                override_expected_pending_persistent_boot_preference: None,
                override_expected_transient_boot_preference: None,
            },
            override_progress_timeout: None,
        };

        desc.setup().await.finish().await.expect_failure(&|error, sp1, sp2| {
            assert_matches!(error, ApplyUpdateError::PreconditionFailed(..));
            let message = InlineErrorChain::new(error).to_string();
            eprintln!("{}", message);
            assert!(message.contains(
                "expected to find inactive version \
                     Version(ArtifactVersion(\"something-else\")), but found \
                     Version(\"0.0.3\")"
            ));

            // No changes should have been made in this case.
            assert_eq!(sp1, sp2);
        });

        // Test a case where we fail to fetch the artifact.  We simulate this by
        // tearing down our artifact server before the update starts.
        let desc = UpdateDescription {
            gwtestctx: &gwtestctx,
            artifacts: &artifacts,
            sp_type: SpType::Sled,
            slot_id: 1,
            artifact_hash: &artifacts.sp_gimlet_artifact_hash,
            override_baseboard_id: None,
            override_expected_sp_component: ExpectedSpComponent::Rot {
                override_expected_active_slot: None,
                override_expected_inactive_version: None,
                override_expected_persistent_boot_preference: None,
                override_expected_pending_persistent_boot_preference: None,
                override_expected_transient_boot_preference: None,
            },
            override_progress_timeout: None,
        };
        let in_progress = desc.setup().await;
        artifacts.teardown().await;
        in_progress.finish().await.expect_failure(&|error, sp1, sp2| {
            assert_matches!(error, ApplyUpdateError::FetchArtifact(..));
            // No changes should have been made in this case.
            assert_eq!(sp1, sp2);
        });

        gwtestctx.teardown().await;
    }

    /// Tests a bunch of easy fast-failure RoT bootloader cases.
    #[tokio::test]
    async fn test_rot_bootloader_basic_failures() {
        let gwtestctx = gateway_test_utils::setup::test_setup(
            "test_rot_bootloader_basic_failures",
            SpPort::One,
        )
        .await;
        let log = &gwtestctx.logctx.log;
        let artifacts = TestArtifacts::new(log).await.unwrap();

        // Test a case of mistaken identity (reported baseboard does not match
        // the one that we expect).
        let desc = UpdateDescription {
            gwtestctx: &gwtestctx,
            artifacts: &artifacts,
            sp_type: SpType::Sled,
            slot_id: 1,
            artifact_hash: &artifacts.sp_gimlet_artifact_hash,
            override_baseboard_id: Some(BaseboardId {
                part_number: String::from("i86pc"),
                serial_number: String::from("SimGimlet0"),
            }),
            override_expected_sp_component:
                ExpectedSpComponent::RotBootloader {
                    override_expected_stage0: None,
                    override_expected_stage0_next: None,
                },
            override_progress_timeout: None,
        };

        desc.setup().await.finish().await.expect_failure(&|error, sp1, sp2| {
            assert_matches!(error, ApplyUpdateError::PreconditionFailed(..));
            let message = InlineErrorChain::new(error).to_string();
            eprintln!("{}", message);
            assert!(message.contains(
                "in sled slot 1, expected to find part \"i86pc\" serial \
                         \"SimGimlet0\", but found part \"i86pc\" serial \
                         \"SimGimlet01\"",
            ));

            // No changes should have been made in this case.
            assert_eq!(sp1, sp2);
        });

        // Test a case where the active version doesn't match what we expect.
        let desc = UpdateDescription {
            gwtestctx: &gwtestctx,
            artifacts: &artifacts,
            sp_type: SpType::Sled,
            slot_id: 1,
            artifact_hash: &artifacts.sp_gimlet_artifact_hash,
            override_baseboard_id: None,
            override_expected_sp_component:
                ExpectedSpComponent::RotBootloader {
                    override_expected_stage0: Some(
                        "not-right".parse().unwrap(),
                    ),
                    override_expected_stage0_next: None,
                },
            override_progress_timeout: None,
        };

        desc.setup().await.finish().await.expect_failure(&|error, sp1, sp2| {
            assert_matches!(error, ApplyUpdateError::PreconditionFailed(..));
            let message = InlineErrorChain::new(error).to_string();
            eprintln!("{}", message);
            assert!(message.contains(
                "expected to find active version \"not-right\", but \
                         found \"0.0.200\""
            ));

            // No changes should have been made in this case.
            assert_eq!(sp1, sp2);
        });

        // Test a case where the inactive version doesn't match what it should
        // (expected invalid, found something else).
        let desc = UpdateDescription {
            gwtestctx: &gwtestctx,
            artifacts: &artifacts,
            sp_type: SpType::Sled,
            slot_id: 1,
            artifact_hash: &artifacts.sp_gimlet_artifact_hash,
            override_baseboard_id: None,
            override_expected_sp_component:
                ExpectedSpComponent::RotBootloader {
                    override_expected_stage0: None,
                    override_expected_stage0_next: Some(
                        ExpectedVersion::NoValidVersion,
                    ),
                },
            override_progress_timeout: None,
        };

        desc.setup().await.finish().await.expect_failure(&|error, sp1, sp2| {
            assert_matches!(error, ApplyUpdateError::PreconditionFailed(..));
            let message = InlineErrorChain::new(error).to_string();
            eprintln!("{}", message);
            assert!(message.contains(
                "expected to find inactive version NoValidVersion, \
                         but found Version(\"0.0.200\")"
            ));

            // No changes should have been made in this case.
            assert_eq!(sp1, sp2);
        });

        // Now test a case where the inactive version doesn't match what it
        // should (expected a different valid version).
        let desc = UpdateDescription {
            gwtestctx: &gwtestctx,
            artifacts: &artifacts,
            sp_type: SpType::Sled,
            slot_id: 1,
            artifact_hash: &artifacts.sp_gimlet_artifact_hash,
            override_baseboard_id: None,
            override_expected_sp_component:
                ExpectedSpComponent::RotBootloader {
                    override_expected_stage0: None,
                    override_expected_stage0_next: Some(
                        ExpectedVersion::Version(
                            "something-else".parse().unwrap(),
                        ),
                    ),
                },
            override_progress_timeout: None,
        };

        desc.setup().await.finish().await.expect_failure(&|error, sp1, sp2| {
                assert_matches!(error, ApplyUpdateError::PreconditionFailed(..));
                let message = InlineErrorChain::new(error).to_string();
                eprintln!("{}", message);
                assert!(message.contains(
                    "expected to find inactive version \
                         Version(ArtifactVersion(\"something-else\")), but found \
                         Version(\"0.0.200\")"
                ));

                // No changes should have been made in this case.
                assert_eq!(sp1, sp2);
            });

        // Test a case where we fail to fetch the artifact.  We simulate this by
        // tearing down our artifact server before the update starts.
        let desc = UpdateDescription {
            gwtestctx: &gwtestctx,
            artifacts: &artifacts,
            sp_type: SpType::Sled,
            slot_id: 1,
            artifact_hash: &artifacts.sp_gimlet_artifact_hash,
            override_baseboard_id: None,
            override_expected_sp_component:
                ExpectedSpComponent::RotBootloader {
                    override_expected_stage0: None,
                    override_expected_stage0_next: None,
                },
            override_progress_timeout: None,
        };
        let in_progress = desc.setup().await;
        artifacts.teardown().await;
        in_progress.finish().await.expect_failure(&|error, sp1, sp2| {
            assert_matches!(error, ApplyUpdateError::FetchArtifact(..));
            // No changes should have been made in this case.
            assert_eq!(sp1, sp2);
        });

        gwtestctx.teardown().await;
    }
}
