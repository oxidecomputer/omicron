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

/// Components for reconfigurator execution.
#[derive(
    Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema,
)]
pub enum ExecutionComponent {
    ExternalNetworking,
    SupportBundles,
    SledList,
    DeployNexusRecords,
    SledAgent,
    PhysicalDisks,
    OmicronZones,
    FirewallRules,
    Dns,
    Cockroach,
    Clickhouse,
    Oximeter,
    MgsUpdates,
}

/// Steps for reconfigurator execution.
#[derive(
    Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema,
)]
pub enum ExecutionStepId {
    /// Fetch information that will be used in subsequent steps.
    Fetch,
    /// Perform cleanup actions on removed items.
    Cleanup,
    /// Idempotent "ensure" or "deploy" step that delegates removes and adds to
    /// other parts of the system.
    Ensure,
}
