// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use oxide_update_engine_types::spec::EngineSpec;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The specification for reconfigurator execution events.
#[derive(JsonSchema)]
pub enum ReconfiguratorExecutionSpec {}

oxide_update_engine::define_update_engine!(pub ReconfiguratorExecutionSpec);
oxide_update_engine_types::define_update_engine_types!(pub ReconfiguratorExecutionSpec);

pub type StepStatus<S = ReconfiguratorExecutionSpec> =
    oxide_update_engine_types::buffer::StepStatus<S>;

impl EngineSpec for ReconfiguratorExecutionSpec {
    fn spec_name() -> String {
        "ReconfiguratorExecutionSpec".to_owned()
    }
    type Component = ExecutionComponent;
    type StepId = ExecutionStepId;
    type StepMetadata = serde_json::Value;
    type ProgressMetadata = serde_json::Value;
    type CompletionMetadata = serde_json::Value;
    type SkippedMetadata = serde_json::Value;
    type Error = anyhow::Error;

    fn rust_type_info()
    -> Option<oxide_update_engine_types::schema::RustTypeInfo> {
        Some(oxide_update_engine_types::schema::RustTypeInfo {
            crate_name: "nexus-types",
            version: "0.1.0",
            path: "nexus_types::deployment::execution\
                   ::ReconfiguratorExecutionSpec",
        })
    }
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
