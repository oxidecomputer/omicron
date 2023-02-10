// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use gateway_client::types::UpdatePreparationProgress;
use omicron_common::api::internal::nexus::UpdateArtifactId;
use schemars::JsonSchema;
use serde::Serialize;
use std::time::Duration;

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct UpdateState {
    pub age: Duration,
    pub kind: UpdateStateKind,
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case", tag = "state", content = "data")]
pub enum UpdateStateKind {
    ResettingSp,
    SendingArtifactToMgs {
        artifact: UpdateArtifactId,
    },
    PreparingForArtifact {
        artifact: UpdateArtifactId,
        progress: Option<UpdatePreparationProgress>,
    },
    ArtifactUpdateProgress {
        bytes_received: u64,
        total_bytes: u64,
    },
    WaitingForStatus {
        artifact: UpdateArtifactId,
    },
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct UpdateEvent {
    pub age: Duration,
    pub kind: UpdateEventKind,
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "data")]
pub enum UpdateEventKind {
    Success(UpdateEventSuccessKind),
    Failure(UpdateEventFailureKind),
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "data")]
pub enum UpdateEventSuccessKind {
    SpResetComplete,
    ArtifactUpdateComplete { artifact: UpdateArtifactId },
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "data")]
pub enum UpdateEventFailureKind {
    SpResetFailed { reason: String },
    ArtifactUpdateFailed { artifact: UpdateArtifactId, reason: String },
}

#[derive(Clone, Debug, Default, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct UpdateLog {
    pub current: Option<UpdateState>,
    pub events: Vec<UpdateEvent>,
}
