// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use dropshot::ErrorStatusCode;
use dropshot::HttpResponseError;
use omicron_uuid_kinds::MupdateUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tufaceous_artifact::ArtifactHash;
use uuid::Uuid;

/// The error type returned by the `sp_component_reset()` MGS endpoint.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, JsonSchema, thiserror::Error,
)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum SpComponentResetError {
    /// MGS refuses to reset its own sled's SP.
    #[error("cannot reset SP of the sled hosting MGS")]
    ResetSpOfLocalSled,

    /// Other dropshot errors.
    #[error("{internal_message}")]
    Other {
        message: String,
        error_code: Option<String>,

        // Skip serializing these fields, as they are used for the
        // `fmt::Display` implementation and for determining the status code,
        // respectively, rather than included in the response body:
        #[serde(skip)]
        internal_message: String,
        #[serde(skip)]
        status: ErrorStatusCode,
    },
}

impl HttpResponseError for SpComponentResetError {
    fn status_code(&self) -> ErrorStatusCode {
        match self {
            SpComponentResetError::ResetSpOfLocalSled => {
                ErrorStatusCode::BAD_REQUEST
            }
            SpComponentResetError::Other { status, .. } => *status,
        }
    }
}

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

#[derive(Deserialize, JsonSchema)]
pub struct ComponentUpdateIdSlot {
    /// An identifier for this update.
    ///
    /// This ID applies to this single instance of the API call; it is not an
    /// ID of `image` itself. Multiple API calls with the same `image` should
    /// use different IDs.
    pub id: Uuid,
    /// The update slot to apply this image to. Supply 0 if the component only
    /// has one update slot.
    pub firmware_slot: u16,
}

#[derive(Deserialize, JsonSchema)]
pub struct UpdateAbortBody {
    /// The ID of the update to abort.
    ///
    /// If the SP is currently receiving an update with this ID, it will be
    /// aborted.
    ///
    /// If the SP is currently receiving an update with a different ID, the
    /// abort request will fail.
    ///
    /// If the SP is not currently receiving any update, the request to abort
    /// should succeed but will not have actually done anything.
    pub id: Uuid,
}
