// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Deployment unit dependency graph for Omicron.
//!
//! This crate defines the DAG of deployment units and their update
//! ordering constraints, shared between `omicron-ls-apis` (which
//! generates the graph) and consumers such as the reconfigurator
//! planner tests.

#[macro_use]
extern crate newtype_derive;

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// The path to omicron-ls-apis relative to the workspace root.
pub const OMICRON_LS_APIS_PATH: &str = "dev-tools/ls-apis";

/// The path to the `deployment_unit_dag.toml` file relative to the
/// omicron-ls-apis directory.
pub const DEPLOYMENT_UNIT_DAG_PATH: &str =
    "tests/output/deployment-unit-dag.toml";

/// Human-readable name for a deployment unit (e.g. "Nexus", "DNS Server").
#[derive(
    Clone, Deserialize, Serialize, Hash, Ord, PartialOrd, Eq, PartialEq,
)]
#[serde(transparent)]
pub struct DeploymentUnitName(String);
NewtypeDebug! { () pub struct DeploymentUnitName(String); }
NewtypeDeref! { () pub struct DeploymentUnitName(String); }
NewtypeDisplay! { () pub struct DeploymentUnitName(String); }
NewtypeFrom! { () pub struct DeploymentUnitName(String); }

impl From<&str> for DeploymentUnitName {
    fn from(s: &str) -> Self {
        DeploymentUnitName(s.to_owned())
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
    pub consumer: DeploymentUnitName,
    pub producer: DeploymentUnitName,
}

/// Top-level structure of the `deployment_unit_dag.toml` file.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DagEdgesFile {
    /// Deployment units that have no edges in the update DAG.
    ///
    /// A deployment unit ends up here when none of its APIs are
    /// server-side-versioned, which means its updates aren't subject to
    /// ordering constraints relative to other units.
    pub units_without_server_side_apis: BTreeSet<DeploymentUnitName>,
    /// Edges of the deployment unit dependency DAG.
    pub edges: BTreeSet<DagEdge>,
}
