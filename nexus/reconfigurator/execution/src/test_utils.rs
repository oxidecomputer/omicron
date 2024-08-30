// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Test utilities for reconfigurator execution.

use internal_dns::resolver::Resolver;
use nexus_db_queries::{context::OpContext, db::DataStore};
use nexus_types::deployment::Blueprint;
use uuid::Uuid;

use crate::{overridables::Overridables, RealizeBlueprintOutput};

pub(crate) async fn realize_blueprint_and_expect(
    opctx: &OpContext,
    datastore: &DataStore,
    resolver: &Resolver,
    blueprint: &Blueprint,
    overrides: &Overridables,
) -> RealizeBlueprintOutput {
    let output = crate::realize_blueprint_with_overrides(
        opctx,
        datastore,
        resolver,
        blueprint,
        Uuid::new_v4(),
        overrides,
    )
    .await
    // We expect here rather than in the caller because we want to assert that
    // the result is a `RealizeBlueprintOutput`. Because the latter is
    // `must_use`, the caller may assign it to `_` and miss the `expect` call.
    .expect("failed to execute blueprint");

    eprintln!("realize_blueprint output: {:#?}", output);
    output
}
