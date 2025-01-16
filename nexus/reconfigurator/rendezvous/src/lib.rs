// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Reconcilation of blueprints (the intended state of the system) and inventory
//! (collected state of the system) into rendezvous tables.
//!
//! Rendezvous tables reflect resources that are in service and available for
//! other parts of Nexus to use. See RFD 541 for more background.

use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintDatasetFilter;
use nexus_types::inventory::Collection;

mod debug_dataset;

pub async fn reconcile_blueprint_rendezvous_tables(
    opctx: &OpContext,
    datastore: &DataStore,
    blueprint: &Blueprint,
    inventory: &Collection,
) -> anyhow::Result<()> {
    debug_dataset::reconcile_debug_datasets(
        opctx,
        datastore,
        blueprint.id,
        blueprint
            .all_omicron_datasets(BlueprintDatasetFilter::All)
            .map(|(_sled_id, dataset)| dataset),
        &inventory
            .sled_agents
            .values()
            .flat_map(|sled| sled.datasets.iter().flat_map(|d| d.id))
            .collect(),
    )
    .await?;

    Ok(())
}
