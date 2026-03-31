// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use serde::{Deserialize, Serialize};

/// The path to omicron-ls-apis relative to the workspace root.
pub const OMICRON_LS_APIS_PATH: &str = "dev-tools/ls-apis";

/// The path to the `deployment_unit_dag.toml` file relative to the
/// omicron-ls-apis directory.
pub const DEPLOYMENT_UNIT_DAG_PATH: &str =
    "tests/output/deployment-unit-dag.toml";

/// Short, machine-friendly identifier for a deployment unit
/// (e.g. "nexus", "dns_server").
#[derive(
    Clone, Deserialize, Serialize, Hash, Ord, PartialOrd, Eq, PartialEq,
)]
#[serde(transparent)]
pub struct DeploymentUnitId(String);
NewtypeDebug! { () pub struct DeploymentUnitId(String); }
NewtypeDeref! { () pub struct DeploymentUnitId(String); }
NewtypeDisplay! { () pub struct DeploymentUnitId(String); }
NewtypeFrom! { () pub struct DeploymentUnitId(String); }

impl From<&str> for DeploymentUnitId {
    fn from(s: &str) -> Self {
        DeploymentUnitId(s.to_owned())
    }
}

/// A single directed edge in the deployment unit dependency DAG.
///
/// `consumer` depends on `producer`: the producer must be fully updated
/// before the consumer starts updating.
#[derive(
    Clone, Debug, Deserialize, Serialize, Eq, PartialEq, Ord, PartialOrd,
)]
pub struct DagEdge {
    pub consumer: DeploymentUnitId,
    pub producer: DeploymentUnitId,
}

/// Top-level structure of the `deployment_unit_dag.toml` file.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DagEdgesFile {
    pub edges: Vec<DagEdge>,
}
