// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Drive one or more in-progress MGS-managed updates

use crate::{
    MgsClients,
    common_sp_update::{
        ReconfiguratorSpComponentUpdater, STATUS_POLL_INTERVAL, VersionStatus,
    },
    mgs_clients::GatewayClientError,
};
use gateway_client::types::SpUpdateStatus;
use nexus_types::deployment::PendingMgsUpdate;
use slog::error;
use slog::{debug, info, warn};
use slog_error_chain::InlineErrorChain;
use std::time::{Duration, Instant};
use thiserror::Error;
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
