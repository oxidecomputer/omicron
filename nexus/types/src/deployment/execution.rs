// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use update_engine::StepSpec;

/// The specification for reconfigurator execution events.
#[derive(JsonSchema)]
pub enum ReconfiguratorExecutionSpec {}

update_engine::define_update_engine!(pub ReconfiguratorExecutionSpec);

impl StepSpec for ReconfiguratorExecutionSpec {
    type Component = ExecutionComponent;
    type StepId = ExecutionStepId;
    type StepMetadata = serde_json::Value;
    type ProgressMetadata = serde_json::Value;
    type CompletionMetadata = serde_json::Value;
    type SkippedMetadata = serde_json::Value;
    type Error = anyhow::Error;
}

#[derive(Debug)]
pub struct ReconfiguratorExecutionErrors {
    // This is a linked list of errors because that's the only format really
    // supported by the Rust std::error::Error API (which follows a chain of
    // borrowed errors to their borrowed sources).
    errors: Vec<anyhow::Error>,
}

/// Components for reconfigurator execution.
#[derive(
    Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema,
)]
pub enum ExecutionComponent {
    ExternalNetworking,
    SledList,
    PhysicalDisks,
    Zones,
    FirewallRules,
    DatasetRecords,
    Dns,
    Cockroach,
}

/// Steps for reconfigurator execution.
#[derive(
    Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema,
)]
pub enum ExecutionStepId {
    /// Fetch information that will be used in subsequent steps.
    Fetch,
    Add,
    Remove,
    /// Idempotent ensure step that delegates removes and adds to other parts
    /// of the system.
    Ensure,
    /// Finalize the blueprint and check for errors at the end of execution.
    Finalize,
}
