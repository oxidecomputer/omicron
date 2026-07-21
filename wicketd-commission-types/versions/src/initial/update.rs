// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Update (mupdate) types for the commissioning API.
//!
//! The progress type here is a flat, server-computed projection of wicketd's
//! internal update-engine event buffers: callers get a single per-SP state plus
//! a human-readable step summary, rather than the full event-report graph.

use std::collections::BTreeSet;

use iddqd::{IdOrdItem, id_upcast};
use schemars::JsonSchema;
use semver::Version;
use serde::{Deserialize, Serialize};

use crate::v1::inventory::SpIdentifier;

/// A non-empty set of service processors to act on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct UpdateTargets(pub(crate) BTreeSet<SpIdentifier>);

impl UpdateTargets {
    pub fn new(
        targets: BTreeSet<SpIdentifier>,
    ) -> Result<Self, EmptyUpdateTargets> {
        if targets.is_empty() {
            Err(EmptyUpdateTargets)
        } else {
            Ok(Self(targets))
        }
    }
}

impl<'de> Deserialize<'de> for UpdateTargets {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let targets = BTreeSet::<SpIdentifier>::deserialize(deserializer)?;
        UpdateTargets::new(targets).map_err(|EmptyUpdateTargets| {
            serde::de::Error::invalid_length(0, &"at least one target")
        })
    }
}

impl JsonSchema for UpdateTargets {
    fn schema_name() -> String {
        "UpdateTargets".to_string()
    }

    fn json_schema(
        generator: &mut schemars::r#gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        use schemars::schema::{
            ArrayValidation, InstanceType, Metadata, Schema, SchemaObject,
        };

        Schema::Object(SchemaObject {
            metadata: Some(Box::new(Metadata {
                description: Some(
                    "A non-empty set of service processors to act on."
                        .to_string(),
                ),
                ..Default::default()
            })),
            instance_type: Some(InstanceType::Array.into()),
            array: Some(Box::new(ArrayValidation {
                items: Some(generator.subschema_for::<SpIdentifier>().into()),
                min_items: Some(1),
                unique_items: Some(true),
                ..Default::default()
            })),
            ..Default::default()
        })
    }
}

/// Error returned when UpdateTargets is constructed from an empty set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmptyUpdateTargets;

impl std::fmt::Display for EmptyUpdateTargets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("at least one target must be specified")
    }
}

impl std::error::Error for EmptyUpdateTargets {}

/// A description of the TUF repository currently held by wicketd.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RepositoryDescription {
    /// The system version of the uploaded TUF repository, if one is present.
    pub system_version: Option<Version>,
}

/// Options controlling how an update is performed.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    Serialize,
    Deserialize,
    JsonSchema,
)]
#[serde(deny_unknown_fields)]
pub struct StartUpdateOptions {
    /// If true, skip the check on the current RoT bootloader version and always
    /// update it regardless of whether the update appears to be needed.
    #[serde(default)]
    pub skip_rot_bootloader_version_check: bool,

    /// If true, skip the check on the current RoT version and always update it
    /// regardless of whether the update appears to be needed.
    #[serde(default)]
    pub skip_rot_version_check: bool,

    /// If true, skip the check on the current SP version and always update it
    /// regardless of whether the update appears to be needed.
    #[serde(default)]
    pub skip_sp_version_check: bool,
}

/// Parameters for starting an update.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StartUpdateParams {
    /// The service processors to update.
    pub targets: UpdateTargets,
    /// Options controlling the update.
    pub options: StartUpdateOptions,
}

/// Parameters for clearing update state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ClearUpdateStateParams {
    /// The service processors to clear update state for.
    pub targets: UpdateTargets,
}

/// Update progress for a single service processor.
///
/// An SP's update progress is only provided once an update has been started for
/// it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SpUpdateProgress {
    /// The service processor this progress describes.
    pub sp: SpIdentifier,
    /// The update progress for that service processor.
    pub progress: UpdateProgress,
}

impl IdOrdItem for SpUpdateProgress {
    type Key<'a> = SpIdentifier;

    fn key(&self) -> Self::Key<'_> {
        self.sp
    }

    id_upcast!();
}

/// The progress of a single update execution: its overall state and its steps.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct UpdateProgress {
    /// The overall rollup state of this execution.
    pub state: UpdateState,
    /// The steps of this execution, in order.
    pub steps: Vec<UpdateStep>,
}

/// The state of an update execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum UpdateState {
    /// An update has been started, but execution has not begun yet.
    Waiting,
    /// The update is currently running.
    Running,
    /// The update ran to completion.
    ///
    /// Whether individual steps performed work or were skipped (for example,
    /// because a component was already at its target version) is recorded in
    /// the per-step outcomes.
    Completed,
    /// The update failed.
    Failed {
        /// A human-readable description of the failure, combining the failed
        /// step and its error message (including any nested causes).
        message: String,
    },
    /// The update was aborted.
    Aborted {
        /// A human-readable description of the abort, combining the aborted
        /// step and its message.
        message: String,
    },
}

/// A single node in the update step tree.
///
/// A step may spawn nested executions (for example, a per-component update that
/// runs its own engine). Those executions appear in `children`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct UpdateStep {
    /// A human-readable description of this step.
    pub description: String,
    /// The status of this step.
    pub status: UpdateStepStatus,
    /// The nested executions this step spawned, in order. Empty if the step ran
    /// no nested engine.
    pub children: Vec<UpdateProgress>,
}

/// The status of a single step in the update step tree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum UpdateStepStatus {
    /// The step has not started yet.
    NotStarted,
    /// The step is currently running.
    Running {
        /// The most recent progress reported by the step, if any.
        progress: Option<StepProgress>,
    },
    /// The step completed.
    Completed {
        /// The outcome of the completed step.
        outcome: StepOutcome,
    },
    /// The step failed.
    Failed {
        /// A human-readable description of the failure.
        message: String,
        /// The chain of underlying causes for the failure, outermost first.
        causes: Vec<String>,
    },
    /// The step was aborted while running.
    Aborted {
        /// A human-readable description of the abort.
        message: String,
    },
    /// The step will not be run because a prior step failed or was aborted.
    WillNotBeRun,
}

/// The outcome of a step that ran to completion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum StepOutcome {
    /// The step succeeded, optionally with a message.
    Success {
        /// An optional message describing the success.
        message: Option<String>,
    },
    /// The step succeeded but produced a warning.
    Warning {
        /// The warning message.
        message: String,
    },
    /// The step was skipped.
    Skipped {
        /// The message describing why the step was skipped.
        message: String,
    },
}

/// A step's progress towards completion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct StepProgress {
    /// The current progress value.
    pub current: u64,
    /// The total value this progress counts towards, if known.
    pub total: Option<u64>,
    /// The units of `current` and `total` (for example, "bytes").
    pub units: String,
}
