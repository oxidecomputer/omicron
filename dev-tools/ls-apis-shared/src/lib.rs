// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types shared between `omicron-ls-apis` and consumers of its output
//! (e.g. the reconfigurator planner tests).

#[macro_use]
extern crate newtype_derive;

mod deployment_unit_dag;

pub use deployment_unit_dag::{
    DEPLOYMENT_UNIT_DAG_PATH, DagEdge, DagEdgesFile, DeploymentUnitId,
    OMICRON_LS_APIS_PATH,
};
