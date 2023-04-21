// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use gateway_client::types::PowerState;
use omicron_common::update::ArtifactId;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;
use thiserror::Error;
use update_engine::StepSpec;

#[derive(JsonSchema)]
pub enum WicketdEngineSpec {}

#[derive(
    Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema,
)]
#[serde(tag = "component", rename_all = "snake_case")]
pub enum UpdateComponent {
    Rot,
    Sp,
    Host,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "id", rename_all = "snake_case")]
pub enum UpdateStepId {
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
pub enum SpComponentUpdateStage {
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
