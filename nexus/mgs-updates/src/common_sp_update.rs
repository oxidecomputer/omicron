// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Module containing implementation details shared amongst all MGS-to-SP-driven
//! updates.

use super::MgsClients;
use crate::host_phase1_updater::ReconfiguratorHostPhase1Updater;
use crate::mgs_clients::GatewaySpComponentResetError;
use crate::mgs_clients::RetryableMgsError;
use crate::rot_bootloader_updater::ReconfiguratorRotBootloaderUpdater;
use crate::rot_updater::ReconfiguratorRotUpdater;
use crate::sp_updater::ReconfiguratorSpUpdater;
use futures::future::BoxFuture;
use gateway_types::rot::RotSlot;
use nexus_types::deployment::ExpectedVersion;
use nexus_types::deployment::PendingMgsUpdate;
use nexus_types::deployment::PendingMgsUpdateDetails;
use nexus_types::inventory::SpType;
use omicron_common::disk::M2Slot;
use slog::error;
use std::net::SocketAddrV6;
use std::time::Duration;
use thiserror::Error;
use tufaceous_artifact::ArtifactHash;
use tufaceous_artifact::ArtifactKind;
use tufaceous_artifact::ArtifactVersion;
use uuid::Uuid;

/// How frequently do we poll MGS for the update progress?
pub(crate) const STATUS_POLL_INTERVAL: Duration = Duration::from_secs(3);

type GatewayClientError = gateway_client::Error<gateway_client::types::Error>;
type SledAgentClientError =
    sled_agent_client::Error<sled_agent_client::types::Error>;

/// Error type returned when an update to a component managed by the SP fails.
///
/// Note that the SP manages itself, as well, so "SP component" here includes
/// the SP.
#[derive(Debug, thiserror::Error)]
pub enum SpComponentUpdateError {
    #[error("error communicating with MGS")]
    MgsCommunication(#[from] GatewayClientError),
    #[error("error resetting component via MGS")]
    MgsResetComponent(#[from] GatewaySpComponentResetError),
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

/// Implementors provide helper functions used while updating a particular SP
/// component
///
/// This trait is object safe; consumers should use `SpComponentUpdateError`,
/// which wraps an implementor of this trait.
pub trait SpComponentUpdateHelperImpl {
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

/// Provides helper functions used while updating a particular SP component
pub struct SpComponentUpdateHelper {
    inner: Box<dyn SpComponentUpdateHelperImpl + Send + Sync>,
}

impl SpComponentUpdateHelper {
    /// Construct a update helper for a specific kind of update
    pub fn new(details: &PendingMgsUpdateDetails) -> Self {
        let inner: Box<dyn SpComponentUpdateHelperImpl + Send + Sync> =
            match details {
                PendingMgsUpdateDetails::Sp(details) => {
                    Box::new(ReconfiguratorSpUpdater::new(details.clone()))
                }
                PendingMgsUpdateDetails::Rot(details) => {
                    Box::new(ReconfiguratorRotUpdater::new(details.clone()))
                }
                PendingMgsUpdateDetails::RotBootloader(details) => Box::new(
                    ReconfiguratorRotBootloaderUpdater::new(details.clone()),
                ),
                PendingMgsUpdateDetails::HostPhase1(details) => Box::new(
                    ReconfiguratorHostPhase1Updater::new(details.clone()),
                ),
            };
        Self { inner }
    }

    /// Checks if the component is already updated or ready for update
    pub async fn precheck(
        &self,
        log: &slog::Logger,
        mgs_clients: &mut MgsClients,
        update: &PendingMgsUpdate,
    ) -> Result<PrecheckStatus, PrecheckError> {
        self.inner.precheck(log, mgs_clients, update).await
    }

    /// Attempts once to perform any post-update actions (e.g., reset the
    /// device)
    pub async fn post_update(
        &self,
        log: &slog::Logger,
        mgs_clients: &mut MgsClients,
        update: &PendingMgsUpdate,
    ) -> Result<(), PostUpdateError> {
        self.inner.post_update(log, mgs_clients, update).await
    }
}

/// Describes the live state of the component before the update begins
#[derive(Debug)]
pub enum PrecheckStatus {
    UpdateComplete,
    ReadyForUpdate,
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

    #[error("fetching inventory from sled-agent at {address}")]
    SledAgentInventory {
        address: SocketAddrV6,
        #[source]
        err: SledAgentClientError,
    },

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

    #[error(
        "expected to find active RoT slot {expected:?}, but found {found:?}"
    )]
    WrongActiveRotSlot { expected: RotSlot, found: RotSlot },

    #[error(
        "expected to find active version {:?}, but found {found:?}",
        .expected.as_str(),
    )]
    WrongActiveVersion { expected: ArtifactVersion, found: String },

    #[error(
        "expected to find active {kind} artifact {expected}, but found {found}"
    )]
    WrongActiveArtifact {
        kind: ArtifactKind,
        expected: ArtifactHash,
        found: ArtifactHash,
    },

    #[error("failed to determine current active {kind} artifact: {err}")]
    DeterminingActiveArtifact { kind: ArtifactKind, err: String },

    #[error(
        "expected to find inactive version {expected:?}, but found {found:?}"
    )]
    WrongInactiveVersion { expected: ExpectedVersion, found: FoundVersion },

    #[error(
        "expected to find inactive {kind} artifact {expected}, \
         but found {found}"
    )]
    WrongInactiveArtifact {
        kind: ArtifactKind,
        expected: ArtifactHash,
        found: ArtifactHash,
    },

    #[error(
        "failed to determine current inactive host OS phase 2 artifact: {err}"
    )]
    DeterminingInactiveHostPhase2 { err: String },

    #[error("inventory missing `last_reconciliation` result")]
    SledAgentInventoryMissingLastReconciliation,

    #[error(
        "invalid host phase 1 slot reported by SP (expected 0 or 1, got {slot})"
    )]
    InvalidHostPhase1Slot { slot: u16 },

    #[error(
        "expected to find active host phase 1 slot {expected}, \
         but found {found}"
    )]
    WrongActiveHostPhase1Slot { expected: M2Slot, found: M2Slot },

    #[error(
        "expected to find host OS boot disk {expected:?}, but found {found:?}"
    )]
    WrongHostOsBootDisk { expected: M2Slot, found: M2Slot },

    #[error(
        "active phase 1 slot {phase1:?} does not match boot disk {boot_disk:?}"
    )]
    MismatchedHostOsActiveSlot { phase1: M2Slot, boot_disk: M2Slot },

    #[error("inventory reported an error determining boot disk: {err}")]
    DeterminingHostOsBootDisk { err: String },
}

#[derive(Debug, thiserror::Error)]
pub enum PostUpdateError {
    #[error("communicating with MGS")]
    GatewayClientError(#[from] GatewayClientError),

    #[error("resetting component via MGS")]
    GatewaySpComponentResetError(#[from] GatewaySpComponentResetError),

    #[error("transient error: {message:?}")]
    TransientError { message: String },

    #[error("fatal error: {error:?}")]
    FatalError { error: String },
}

impl PostUpdateError {
    pub fn is_fatal(&self) -> bool {
        match self {
            PostUpdateError::GatewayClientError(error) => {
                !error.should_try_next_mgs()
            }
            PostUpdateError::GatewaySpComponentResetError(error) => {
                !error.should_try_next_mgs()
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
        &self,
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
                    found: self.clone(),
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
        || message.contains("failed to read data from the caboose")
}
