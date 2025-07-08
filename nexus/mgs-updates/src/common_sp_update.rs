// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Module containing implementation details shared amongst all MGS-to-SP-driven
//! updates.

use super::MgsClients;
use super::UpdateProgress;
use futures::future::BoxFuture;
use gateway_client::types::SpType;
use gateway_client::types::SpUpdateStatus;
use gateway_types::rot::RotSlot;
use nexus_types::deployment::ExpectedVersion;
use nexus_types::deployment::PendingMgsUpdate;
use slog::Logger;
use slog::{debug, error, info, warn};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::watch;
use tufaceous_artifact::ArtifactVersion;
use uuid::Uuid;

/// How frequently do we poll MGS for the update progress?
pub(crate) const STATUS_POLL_INTERVAL: Duration = Duration::from_secs(3);

type GatewayClientError = gateway_client::Error<gateway_client::types::Error>;

/// Error type returned when an update to a component managed by the SP fails.
///
/// Note that the SP manages itself, as well, so "SP component" here includes
/// the SP.
#[derive(Debug, thiserror::Error)]
pub enum SpComponentUpdateError {
    #[error("error communicating with MGS")]
    MgsCommunication(#[from] GatewayClientError),
    #[error("different update is now preparing ({0})")]
    DifferentUpdatePreparing(Uuid),
    #[error("different update is now in progress ({0})")]
    DifferentUpdateInProgress(Uuid),
    #[error("different update is now complete ({0})")]
    DifferentUpdateComplete(Uuid),
    #[error("different update is now aborted ({0})")]
    DifferentUpdateAborted(Uuid),
    #[error("different update failed ({0})")]
    DifferentUpdateFailed(Uuid),
    #[error("update status lost (did the SP reset?)")]
    UpdateStatusLost,
    #[error("update was aborted")]
    UpdateAborted,
    #[error("update failed (error code {0})")]
    UpdateFailedWithCode(u32),
    #[error("update failed (error message {0})")]
    UpdateFailedWithMessage(String),
}

/// Describes an update to a component for which the SP drives the update
///
/// This trait is essentially historical at this point.  We maintain impls so
/// that we have tested reference implementations.  But these will eventually be
/// migrated to `ReconfiguratorSpComponentUpdater` instead.
pub trait SpComponentUpdater {
    /// The target component.
    ///
    /// Should be produced via `SpComponent::const_as_str()`.
    fn component(&self) -> &'static str;

    /// The type of the target SP.
    fn target_sp_type(&self) -> SpType;

    /// The slot number of the target SP.
    fn target_sp_slot(&self) -> u16;

    /// The target firmware slot for the component.
    fn firmware_slot(&self) -> u16;

    /// The ID of this update.
    fn update_id(&self) -> Uuid;

    /// The update payload data to send to MGS.
    // TODO-performance This has to be convertible into a `reqwest::Body`, so we
    // return an owned Vec. That requires all our implementors to clone the data
    // at least once; maybe we should use `Bytes` instead (which is cheap to
    // clone and also convertible into a reqwest::Body)?
    fn update_data(&self) -> Vec<u8>;

    /// The sending half of the watch channel to report update progress.
    fn progress(&self) -> &watch::Sender<Option<UpdateProgress>>;

    /// Logger to use while performing this update.
    fn logger(&self) -> &Logger;
}

pub(super) async fn deliver_update(
    updater: &(dyn SpComponentUpdater + Send + Sync),
    mgs_clients: &mut MgsClients,
) -> Result<(), SpComponentUpdateError> {
    // Start the update.
    mgs_clients
        .try_all_serially(updater.logger(), |client| async move {
            client
                .sp_component_update(
                    updater.target_sp_type(),
                    updater.target_sp_slot(),
                    updater.component(),
                    updater.firmware_slot(),
                    &updater.update_id(),
                    reqwest::Body::from(updater.update_data()),
                )
                .await?;
            updater.progress().send_replace(Some(UpdateProgress::Started));
            info!(
                updater.logger(), "update started";
                "mgs_addr" => client.baseurl(),
            );
            Ok(())
        })
        .await?;

    // Wait for the update to complete.
    loop {
        let status = mgs_clients
            .try_all_serially(updater.logger(), |client| async move {
                let update_status = client
                    .sp_component_update_status(
                        updater.target_sp_type(),
                        updater.target_sp_slot(),
                        updater.component(),
                    )
                    .await?;

                debug!(
                    updater.logger(), "got update status";
                    "mgs_addr" => client.baseurl(),
                    "status" => ?update_status,
                );

                Ok(update_status)
            })
            .await?;

        if status_is_complete(
            status.into_inner(),
            updater.update_id(),
            updater.progress(),
            updater.logger(),
        )? {
            updater.progress().send_replace(Some(UpdateProgress::InProgress {
                progress: Some(1.0),
            }));
            return Ok(());
        }

        tokio::time::sleep(STATUS_POLL_INTERVAL).await;
    }
}

fn status_is_complete(
    status: SpUpdateStatus,
    update_id: Uuid,
    progress_tx: &watch::Sender<Option<UpdateProgress>>,
    log: &Logger,
) -> Result<bool, SpComponentUpdateError> {
    match status {
        // For `Preparing` and `InProgress`, we could check the progress
        // information returned by these steps and try to check that
        // we're still _making_ progress, but every Nexus instance needs
        // to do that anyway in case we (or the MGS instance delivering
        // the update) crash, so we'll omit that check here. Instead, we
        // just sleep and we'll poll again shortly.
        SpUpdateStatus::Preparing { id, progress } => {
            if id == update_id {
                let progress = progress.and_then(|progress| {
                    if progress.current > progress.total {
                        warn!(
                            log, "nonsense preparing progress";
                            "current" => progress.current,
                            "total" => progress.total,
                        );
                        None
                    } else if progress.total == 0 {
                        None
                    } else {
                        Some(
                            f64::from(progress.current)
                                / f64::from(progress.total),
                        )
                    }
                });
                progress_tx
                    .send_replace(Some(UpdateProgress::Preparing { progress }));
                Ok(false)
            } else {
                Err(SpComponentUpdateError::DifferentUpdatePreparing(id))
            }
        }
        SpUpdateStatus::InProgress { id, bytes_received, total_bytes } => {
            if id == update_id {
                let progress = if bytes_received > total_bytes {
                    warn!(
                        log, "nonsense update progress";
                        "bytes_received" => bytes_received,
                        "total_bytes" => total_bytes,
                    );
                    None
                } else if total_bytes == 0 {
                    None
                } else {
                    Some(f64::from(bytes_received) / f64::from(total_bytes))
                };
                progress_tx.send_replace(Some(UpdateProgress::InProgress {
                    progress,
                }));
                Ok(false)
            } else {
                Err(SpComponentUpdateError::DifferentUpdateInProgress(id))
            }
        }
        SpUpdateStatus::Complete { id } => {
            if id == update_id {
                Ok(true)
            } else {
                Err(SpComponentUpdateError::DifferentUpdateComplete(id))
            }
        }
        SpUpdateStatus::None => Err(SpComponentUpdateError::UpdateStatusLost),
        SpUpdateStatus::Aborted { id } => {
            if id == update_id {
                Err(SpComponentUpdateError::UpdateAborted)
            } else {
                Err(SpComponentUpdateError::DifferentUpdateAborted(id))
            }
        }
        SpUpdateStatus::Failed { code, id } => {
            if id == update_id {
                Err(SpComponentUpdateError::UpdateFailedWithCode(code))
            } else {
                Err(SpComponentUpdateError::DifferentUpdateFailed(id))
            }
        }
        SpUpdateStatus::RotError { id, message } => {
            if id == update_id {
                Err(SpComponentUpdateError::UpdateFailedWithMessage(format!(
                    "rot error: {message}"
                )))
            } else {
                Err(SpComponentUpdateError::DifferentUpdateFailed(id))
            }
        }
    }
}

/// Provides helper functions used while updating a particular SP component
pub trait SpComponentUpdateHelper {
    /// Checks if the component is already updated or ready for update
    fn precheck<'a>(
        &'a self,
        log: &'a slog::Logger,
        mgs_clients: &'a mut MgsClients,
        update: &'a PendingMgsUpdate,
    ) -> BoxFuture<'a, Result<PrecheckStatus, PrecheckError>>;

    /// Attempts once to perform any post-update actions (e.g., reset the
    /// device)
    fn post_update<'a>(
        &'a self,
        log: &'a slog::Logger,
        mgs_clients: &'a mut MgsClients,
        update: &'a PendingMgsUpdate,
    ) -> BoxFuture<'a, Result<(), PostUpdateError>>;
}

/// Describes the live state of the component before the update begins
#[derive(Debug)]
pub enum PrecheckStatus {
    UpdateComplete,
    ReadyForUpdate,
    WaitingForOngoingUpdate,
}

#[derive(Debug, Error)]
pub enum PrecheckError {
    #[error(
        "pending_persistent_boot_preference and/or transient_boot_preference is set"
    )]
    EphemeralRotBootPreferenceSet,

    #[error("communicating with MGS")]
    GatewayClientError(#[from] GatewayClientError),

    #[error("communicating with RoT: {message:?}")]
    RotCommunicationFailed { message: String },

    #[error(
        "in {sp_type} slot {slot_id}, expected to find \
         part {expected_part:?} serial {expected_serial:?}, but found \
         part {found_part:?} serial {found_serial:?}"
    )]
    WrongDevice {
        sp_type: SpType,
        slot_id: u16,
        expected_part: String,
        expected_serial: String,
        found_part: String,
        found_serial: String,
    },

    #[error("expected to find active slot {expected:?}, but found {found:?}")]
    WrongActiveSlot { expected: RotSlot, found: RotSlot },

    #[error(
        "expected to find active version {:?}, but found {found:?}",
        .expected.as_str(),
    )]
    WrongActiveVersion { expected: ArtifactVersion, found: String },

    #[error(
        "expected to find inactive version {expected:?}, but found {found:?}"
    )]
    WrongInactiveVersion { expected: ExpectedVersion, found: FoundVersion },
}

#[derive(Debug, thiserror::Error)]
pub enum PostUpdateError {
    #[error("communicating with MGS")]
    GatewayClientError(#[from] GatewayClientError),

    #[error("transient error: {message:?}")]
    TransientError { message: String },

    #[error("fatal error: {error:?}")]
    FatalError { error: String },
}

impl PostUpdateError {
    pub fn is_fatal(&self) -> bool {
        match self {
            PostUpdateError::GatewayClientError(error) => {
                !matches!(error, gateway_client::Error::CommunicationError(_))
            }
            PostUpdateError::TransientError { .. } => false,
            PostUpdateError::FatalError { .. } => true,
        }
    }
}

#[derive(Debug, Clone)]
pub enum FoundVersion {
    MissingVersion,
    Version(String),
}

impl FoundVersion {
    pub fn matches(
        self,
        expected: &ExpectedVersion,
    ) -> Result<(), PrecheckError> {
        match (expected, &self) {
            // expected garbage, found garbage
            (ExpectedVersion::NoValidVersion, FoundVersion::MissingVersion) => {
                ()
            }
            // expected a specific version and found it
            (
                ExpectedVersion::Version(artifact_version),
                FoundVersion::Version(found_version),
            ) if artifact_version.to_string() == *found_version => (),
            // anything else is a mismatch
            (ExpectedVersion::NoValidVersion, FoundVersion::Version(_))
            | (ExpectedVersion::Version(_), FoundVersion::MissingVersion)
            | (ExpectedVersion::Version(_), FoundVersion::Version(_)) => {
                return Err(PrecheckError::WrongInactiveVersion {
                    expected: expected.clone(),
                    found: self,
                });
            }
        };

        Ok(())
    }
}

pub(crate) fn error_means_caboose_is_invalid(
    error: &GatewayClientError,
) -> bool {
    // This is not great.  See oxidecomputer/omicron#8014.
    let message = format!("{error:?}");
    message.contains("the image caboose does not contain")
        || message.contains("the image does not include a caboose")
}
