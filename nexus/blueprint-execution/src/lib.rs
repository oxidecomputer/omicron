// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Execution of Nexus blueprints
//!
//! See `nexus_deployment` crate-level docs for background.

use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::Blueprint;
use slog::o;

mod omicron_zones;
mod resource_allocation;

/// Make one attempt to realize the given blueprint, meaning to take actions to
/// alter the real system to match the blueprint
///
/// The assumption is that callers are running this periodically or in a loop to
/// deal with transient errors or changes in the underlying system state.
pub async fn realize_blueprint(
    opctx: &OpContext,
    datastore: &DataStore,
    blueprint: &Blueprint,
) -> Result<(), Vec<anyhow::Error>> {
    let log = opctx.log.new(o!("comment" => blueprint.comment.clone()));

    resource_allocation::ensure_zone_resources_allocated(
        &log,
        opctx,
        datastore,
        &blueprint.omicron_zones,
    )
    .await
    .map_err(|err| vec![err])?;

    omicron_zones::deploy_zones(
        &log,
        opctx,
        datastore,
        &blueprint.omicron_zones,
    )
    .await
}
