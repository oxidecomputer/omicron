// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::time::Duration;

use gateway_messages::UpdateStatus;
use omicron_uuid_kinds::MupdateUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tufaceous_artifact::ArtifactHash;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum SpUpdateStatus {
    /// The SP has no update status.
    None,
    /// The SP is preparing to receive an update.
    ///
    /// May or may not include progress, depending on the capabilities of the
    /// component being updated.
    Preparing { id: Uuid, progress: Option<UpdatePreparationProgress> },
    /// The SP is currently receiving an update.
    InProgress { id: Uuid, bytes_received: u32, total_bytes: u32 },
    /// The SP has completed receiving an update.
    Complete { id: Uuid },
    /// The SP has aborted an in-progress update.
    Aborted { id: Uuid },
    /// The update process failed.
    Failed { id: Uuid, code: u32 },
    /// The update process failed with an RoT-specific error.
    RotError { id: Uuid, message: String },
}

impl From<UpdateStatus> for SpUpdateStatus {
    fn from(status: UpdateStatus) -> Self {
        match status {
            UpdateStatus::None => Self::None,
            UpdateStatus::Preparing(status) => Self::Preparing {
                id: status.id.into(),
                progress: status.progress.map(Into::into),
            },
            UpdateStatus::SpUpdateAuxFlashChckScan {
                id, total_size, ..
            } => Self::InProgress {
                id: id.into(),
                bytes_received: 0,
                total_bytes: total_size,
            },
            UpdateStatus::InProgress(status) => Self::InProgress {
                id: status.id.into(),
                bytes_received: status.bytes_received,
                total_bytes: status.total_size,
            },
            UpdateStatus::Complete(id) => Self::Complete { id: id.into() },
            UpdateStatus::Aborted(id) => Self::Aborted { id: id.into() },
            UpdateStatus::Failed { id, code } => {
                Self::Failed { id: id.into(), code }
            }
            UpdateStatus::RotError { id, error } => {
                Self::RotError { id: id.into(), message: format!("{error:?}") }
            }
        }
    }
}

/// Progress of an SP preparing to update.
///
/// The units of `current` and `total` are unspecified and defined by the SP;
/// e.g., if preparing for an update requires erasing a flash device, this may
/// indicate progress of that erasure without defining units (bytes, pages,
/// sectors, etc.).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub struct UpdatePreparationProgress {
    pub current: u32,
    pub total: u32,
}

impl From<gateway_messages::UpdatePreparationProgress>
    for UpdatePreparationProgress
{
    fn from(progress: gateway_messages::UpdatePreparationProgress) -> Self {
        Self { current: progress.current, total: progress.total }
    }
}

// This type is a duplicate of the type in `ipcc`. We keep these types distinct
// to allow us to choose different representations for MGS's HTTP API (this
// type) and the wire format passed through the SP to installinator
// (`ipcc::InstallinatorImageId`), although _currently_ they happen to be
// defined identically.
//
// We don't define a conversion from `Self` to `ipcc::InstallinatorImageId` here
// to avoid a dependency on `libipcc`. Instead, callers can easily perform
// conversions themselves.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub struct InstallinatorImageId {
    pub update_id: MupdateUuid,
    pub host_phase_2: ArtifactHash,
    pub control_plane: ArtifactHash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "progress", rename_all = "snake_case")]
pub enum HostPhase2Progress {
    Available {
        image_id: HostPhase2RecoveryImageId,
        offset: u64,
        total_size: u64,
        age: Duration,
    },
    None,
}

/// Identity of a host phase2 recovery image.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct HostPhase2RecoveryImageId {
    pub sha256_hash: ArtifactHash,
}
