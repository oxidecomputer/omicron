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
use std::sync::Arc;
use thiserror::Error;
use update_engine::StepSpec;
use update_engine::errors::NestedEngineError;

#[derive(JsonSchema)]
pub enum WicketdEngineSpec {}

#[derive(
    Copy,
    Clone,
    Debug,
    Eq,
    PartialEq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    JsonSchema,
)]
#[serde(tag = "component", rename_all = "snake_case")]
pub enum UpdateComponent {
    RotBootloader,
    Rot,
    Sp,
    Host,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "id", rename_all = "snake_case")]
pub enum UpdateStepId {
    TestStep,
    SetHostPowerState { state: PowerState },
    InterrogateRotBootloader,
    InterrogateRot,
    InterrogateSp,
    SpComponentUpdate,
    SettingInstallinatorImageId,
    ClearingInstallinatorImageId,
    SettingHostStartupOptions,
    WaitingForTrampolinePhase2Upload,
    FetchHostType,
    CheckHostType,
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

update_engine::define_update_engine!(pub WicketdEngineSpec);

#[derive(JsonSchema)]
pub enum TestStepSpec {}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TestStepComponent {
    Test,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TestStepId {
    Delay,
    Stub,
}

#[derive(Debug, Error)]
pub enum TestStepError {}

impl update_engine::AsError for TestStepError {
    fn as_error(&self) -> &(dyn std::error::Error + 'static) {
        self
    }
}

impl StepSpec for TestStepSpec {
    type Component = TestStepComponent;
    type StepId = TestStepId;
    type StepMetadata = serde_json::Value;
    type ProgressMetadata = serde_json::Value;
    type CompletionMetadata = serde_json::Value;
    type SkippedMetadata = serde_json::Value;
    type Error = TestStepError;
}

#[derive(JsonSchema)]
pub enum SpComponentUpdateSpec {}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "id", rename_all = "snake_case")]
pub enum SpComponentUpdateStepId {
    Sending,
    Preparing,
    Writing,
    SettingActiveBootSlot,
    Resetting,
    CheckingActiveBootSlot,
}

impl StepSpec for SpComponentUpdateSpec {
    type Component = UpdateComponent;
    type StepId = SpComponentUpdateStepId;
    type StepMetadata = serde_json::Value;
    type ProgressMetadata = serde_json::Value;
    type CompletionMetadata = serde_json::Value;
    type SkippedMetadata = serde_json::Value;
    type Error = SpComponentUpdateTerminalError;
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
    #[error("getting currently-active RoT slot failed")]
    GetRotActiveSlotFailed {
        #[source]
        error: anyhow::Error,
    },
    #[error("error in test step")]
    TestStepError {
        #[from]
        error: NestedEngineError<TestStepSpec>,
    },
    #[error("simulated failure result")]
    SimulatedFailure,
    #[error("error updating component")]
    ComponentNestedError {
        #[from]
        error: NestedEngineError<SpComponentUpdateSpec>,
    },
    #[error("getting RoT caboose failed")]
    GetRotCabooseFailed {
        #[source]
        error: gateway_client::Error<gateway_client::types::Error>,
    },
    #[error("getting RoT CMPA failed")]
    GetRotCmpaFailed {
        #[source]
        error: anyhow::Error,
    },
    #[error("getting RoT CFPA failed")]
    GetRotCfpaFailed {
        #[source]
        error: anyhow::Error,
    },
    #[error("failed to find correctly-signed RoT image")]
    FailedFindingSignedRotImage {
        #[source]
        error: anyhow::Error,
    },
    #[error("getting SP caboose failed")]
    GetSpCabooseFailed {
        #[source]
        error: gateway_client::Error<gateway_client::types::Error>,
    },
    #[error("TUF repository missing SP image for board {board}")]
    MissingSpImageForBoard { board: String },
    #[error("setting installinator image ID failed")]
    SetInstallinatorImageIdFailed {
        #[source]
        error: gateway_client::Error<gateway_client::types::Error>,
    },
    #[error("setting host boot flash slot failed")]
    SetHostBootFlashSlotFailed {
        #[source]
        error: anyhow::Error,
    },
    #[error("setting host startup options failed for {description}")]
    SetHostStartupOptionsFailed {
        description: &'static str,
        #[source]
        error: gateway_client::Error<gateway_client::types::Error>,
    },
    #[error("uploading trampoline phase 2 to MGS failed")]
    TrampolinePhase2UploadFailed {
        #[source]
        error: Arc<anyhow::Error>,
    },
    #[error(
        "uploading trampoline phase 2 to MGS cancelled (was a new TUF repo uploaded?)"
    )]
    TrampolinePhase2UploadCancelled,
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
    #[error("Can't update Cosmo")]
    CosmoHost,
    #[error("getting SP state failed")]
    SpGetFailed {
        #[source]
        error: gateway_client::Error<gateway_client::types::Error>,
    },
    #[error("Unknown host type {0}")]
    UnknownHost(String),
}

impl update_engine::AsError for UpdateTerminalError {
    fn as_error(&self) -> &(dyn std::error::Error + 'static) {
        self
    }
}

#[derive(Debug, Error)]
pub enum SpComponentUpdateTerminalError {
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
    #[error("setting currently-active RoT slot failed")]
    SetRotActiveSlotFailed {
        #[source]
        error: anyhow::Error,
    },
    #[error("getting currently-active RoT slot failed")]
    GetRotActiveSlotFailed {
        #[source]
        error: anyhow::Error,
    },
    #[error("resetting RoT failed")]
    RotResetFailed {
        #[source]
        error: anyhow::Error,
    },
    #[error("SP reset failed")]
    SpResetFailed {
        #[source]
        error: anyhow::Error,
    },
    #[error("RoT booted into unexpected slot {active_slot}")]
    RotUnexpectedActiveSlot { active_slot: u16 },
    #[error("Getting RoT boot info failed")]
    GetRotBootInfoFailed {
        #[source]
        error: anyhow::Error,
    },
    #[error("Unexpected error returned from RoT bootloader update")]
    RotBootloaderError {
        #[source]
        error: anyhow::Error,
    },
    #[error("setting currently-active RoT bootloader slot failed")]
    SetRotBootloaderActiveSlotFailed {
        #[source]
        error: anyhow::Error,
    },
}

impl update_engine::AsError for SpComponentUpdateTerminalError {
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
