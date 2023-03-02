// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use camino::Utf8PathBuf;
use gateway_client::types::HostPhase2Progress;
use gateway_client::types::PowerState;
use gateway_client::types::UpdatePreparationProgress;
use omicron_common::update::ArtifactId;
use omicron_common::update::ArtifactKind;
use schemars::gen::SchemaGenerator;
use schemars::schema::Schema;
use schemars::schema::SchemaObject;
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
        artifact: ArtifactId,
    },
    PreparingForArtifact {
        artifact: ArtifactId,
        progress: Option<UpdatePreparationProgress>,
    },
    ArtifactDownloadProgress {
        attempt: usize,
        kind: ArtifactKind,
        downloaded_bytes: u64,
        total_bytes: u64,
        elapsed: Duration,
    },
    ArtifactWriteProgress {
        attempt: usize,
        kind: ArtifactKind,
        #[schemars(schema_with = "path_schema")]
        destination: Option<Utf8PathBuf>,
        written_bytes: u64,
        total_bytes: u64,
        elapsed: Duration,
    },
    InstallinatorFormatProgress {
        attempt: usize,
        #[schemars(schema_with = "path_schema")]
        path: Utf8PathBuf,
        percentage: usize,
        elapsed: Duration,
    },
    WaitingForStatus {
        artifact: ArtifactId,
    },
    SettingHostPowerState {
        power_state: PowerState,
    },
    SettingInstallinatorOptions,
    SettingHostStartupOptions,
    WaitingForTrampolineImageDelivery {
        artifact: ArtifactId,
        progress: HostPhase2Progress,
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
    ArtifactUpdateComplete { artifact: ArtifactId },
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "data")]
pub enum UpdateEventFailureKind {
    SpResetFailed { reason: String },
    ArtifactUpdateFailed { artifact: ArtifactId, reason: String },
}

#[derive(Clone, Debug, Default, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct UpdateLog {
    pub current: Option<UpdateState>,
    pub events: Vec<UpdateEvent>,
}

fn path_schema(gen: &mut SchemaGenerator) -> Schema {
    let mut schema: SchemaObject = <String>::json_schema(gen).into();
    schema.format = Some("Utf8PathBuf".to_owned());
    schema.into()
}
