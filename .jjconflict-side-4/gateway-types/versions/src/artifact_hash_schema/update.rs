// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::time::Duration;

use omicron_uuid_kinds::MupdateUuid;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use tufaceous_artifact::ArtifactHash;

use crate::v1;

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

impl From<v1::update::InstallinatorImageId> for InstallinatorImageId {
    fn from(value: v1::update::InstallinatorImageId) -> Self {
        Self {
            update_id: value.update_id,
            host_phase_2: value.host_phase_2,
            control_plane: value.control_plane,
        }
    }
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

impl From<HostPhase2Progress> for v1::update::HostPhase2Progress {
    fn from(value: HostPhase2Progress) -> Self {
        match value {
            HostPhase2Progress::Available {
                image_id,
                offset,
                total_size,
                age,
            } => Self::Available {
                image_id: image_id.into(),
                offset,
                total_size,
                age,
            },
            HostPhase2Progress::None => Self::None,
        }
    }
}

/// Identity of a host phase2 recovery image.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct HostPhase2RecoveryImageId {
    pub sha256_hash: ArtifactHash,
}

impl From<HostPhase2RecoveryImageId> for v1::update::HostPhase2RecoveryImageId {
    fn from(value: HostPhase2RecoveryImageId) -> Self {
        Self { sha256_hash: value.sha256_hash }
    }
}
