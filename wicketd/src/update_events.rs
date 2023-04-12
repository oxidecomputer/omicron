// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use camino::Utf8PathBuf;
use gateway_client::types::HostPhase2Progress;
use gateway_client::types::PowerState;
use gateway_client::types::UpdatePreparationProgress;
use installinator_common::CompletionEventKind;
use omicron_common::update::ArtifactId;
use omicron_common::update::ArtifactKind;
use schemars::gen::SchemaGenerator;
use schemars::schema::Schema;
use schemars::schema::SchemaObject;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;
use std::time::Duration;
use thiserror::Error;
use update_engine::StepSpec;

#[derive(JsonSchema)]
pub(crate) enum WicketdEngineSpec {}

#[derive(
    Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema,
)]
pub(crate) enum UpdateComponent {
    Rot,
    Sp,
    Host,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "id", rename_all = "snake_case")]
pub(crate) enum UpdateStepId {
    SetHostPowerState { state: PowerState },
    ResettingSp,
    SpComponentUpdate { stage: SpComponentUpdateStage },
    SettingInstallinatorImageId,
    ClearingInstallinatorImageId,
    SettingHostStartupOptions,
    WaitingForTrampolinePhase2Upload,
    DownloadingInstallinator,
    RunningInstallinator,
}

impl StepSpec for WicketdEngineSpec {
    type Component = UpdateComponent;
    type StepId = UpdateStepId;
    type StepMetadata = serde_json::Value;
    type ProgressMetadata = serde_json::Value;
    type CompletionMetadata = serde_json::Value;
    type SkippedMetadata = serde_json::Value;
    type Error = UpdateTerminalError;
}

update_engine::define_update_engine!(pub(crate) WicketdEngineSpec);

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct UpdateState {
    pub age: Duration,
    pub kind: UpdateStateKind,
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case", tag = "state", content = "data")]
pub enum SpUpdateState {
    SendingArtifactToMgs,
    WaitingForStatus,
    PreparingForArtifact { progress: Option<UpdatePreparationProgress> },
    Writing { written_bytes: u64, total_bytes: u64 },
    // This is only used by the SP.
    ResettingSp,
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case", tag = "state", content = "data")]
pub enum UpdateStateKind {
    // This is a hack to handle cases where a completion has just happened and
    // there's no progress from the next step yet: see
    // https://github.com/oxidecomputer/omicron/pull/2464#discussion_r1123308999
    // for some discussion.
    WaitingForProgress {
        component: String,
    },
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
        #[schemars(schema_with = "path_opt_schema")]
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
    Normal(UpdateNormalEventKind),
    Terminal(UpdateTerminalError),
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "data")]
pub enum UpdateNormalEventKind {
    SpResetComplete,
    ArtifactUpdateComplete { artifact: ArtifactId },
    InstallinatorEvent(CompletionEventKind),
}

#[derive(Debug, Error)]
pub enum UpdateTerminalError {
    #[error("updating power state failed")]
    UpdatePowerStateFailed {
        // Note that this is an HTTP error, which currently prints out the
        // entire cause chain. This doesn't obey the usual Rust rules about
        // printing out the chain in the Display section, but just accept this
        // for now.
        //
        // This is true for all the other gateway_client::Errors in this section
        // as well.
        #[source]
        error: gateway_client::Error<gateway_client::types::Error>,
    },
    #[error("SP reset failed")]
    SpResetFailed {
        #[source]
        error: anyhow::Error,
    },
    #[error("setting installinator image ID failed")]
    SetInstallinatorImageIdFailed {
        #[source]
        error: gateway_client::Error<gateway_client::types::Error>,
    },
    #[error("setting host boot flash slot failed")]
    SetHostBootFlashSlotFailed {
        #[source]
        error: gateway_client::Error<gateway_client::types::Error>,
    },
    #[error("setting host startup options failed for {description}")]
    SetHostStartupOptionsFailed {
        description: &'static str,
        #[source]
        error: gateway_client::Error<gateway_client::types::Error>,
    },
    #[error(
        "SP component update failed at stage \"{stage}\" for {}",
        display_artifact_id(.artifact)
    )]
    SpComponentUpdateFailed {
        stage: SpComponentUpdateStage,
        artifact: ArtifactId,
        #[source]
        error: anyhow::Error,
    },
    #[error("failed to upload trampoline phase 2 to MGS (was a new TUF repo uploaded)?")]
    // XXX should this carry an error message?
    TrampolinePhase2UploadFailed,
    #[error("downloading installinator failed")]
    DownloadingInstallinatorFailed {
        #[source]
        error: anyhow::Error,
    },
    #[error("running installinator failed")]
    RunningInstallinatorFailed {
        #[source]
        error: anyhow::Error,
    },
}

impl update_engine::AsError for UpdateTerminalError {
    fn as_error(&self) -> &(dyn std::error::Error + 'static) {
        self
    }
}

fn display_artifact_id(artifact: &ArtifactId) -> String {
    format!(
        "{}:{} (version {})",
        artifact.kind, artifact.name, artifact.version
    )
}

#[derive(
    Clone, Copy, Debug, Eq, PartialEq, JsonSchema, Deserialize, Serialize,
)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SpComponentUpdateStage {
    Sending,
    Preparing,
    Writing,
}

impl fmt::Display for SpComponentUpdateStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SpComponentUpdateStage::Sending => f.write_str("sending"),
            SpComponentUpdateStage::Preparing => f.write_str("preparing"),
            SpComponentUpdateStage::Writing => f.write_str("writing"),
        }
    }
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

fn path_opt_schema(gen: &mut SchemaGenerator) -> Schema {
    let mut schema: SchemaObject = <Option<String>>::json_schema(gen).into();
    schema.format = Some("Utf8PathBuf".to_owned());
    schema.into()
}
