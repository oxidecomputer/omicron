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
use nexus_types::deployment::BlueprintDatasetDisposition;
use nexus_types::internal_api::background::BlueprintRendezvousStats;
use nexus_types::inventory::Collection;

mod crucible_dataset;
mod debug_dataset;

pub async fn reconcile_blueprint_rendezvous_tables(
    opctx: &OpContext,
    datastore: &DataStore,
    blueprint: &Blueprint,
    inventory: &Collection,
) -> anyhow::Result<BlueprintRendezvousStats> {
    let inventory_dataset_ids = inventory
        .sled_agents
        .iter()
        .flat_map(|sled| sled.datasets.iter().flat_map(|d| d.id))
        .collect();

    let debug_dataset = debug_dataset::reconcile_debug_datasets(
        opctx,
        datastore,
        blueprint.id,
        blueprint
            .all_omicron_datasets(BlueprintDatasetDisposition::any)
            .map(|(_sled_id, dataset)| dataset),
        &inventory_dataset_ids,
    )
    .await?;

    let crucible_dataset = crucible_dataset::record_new_crucible_datasets(
        opctx,
        datastore,
        blueprint
            .all_omicron_datasets(BlueprintDatasetDisposition::any)
            .map(|(_sled_id, dataset)| dataset),
        &inventory_dataset_ids,
    )
    .await?;

    Ok(BlueprintRendezvousStats { debug_dataset, crucible_dataset })
}

#[cfg(test)]
mod tests {
    use nexus_types::deployment::BlueprintDatasetDisposition;
    use omicron_uuid_kinds::{GenericUuid, TypedUuid, TypedUuidKind};
    use test_strategy::Arbitrary;
    use uuid::Uuid;

    // Helpers to describe how a dataset should be prepared for the proptests
    // in our dataset-syncing submodules.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Arbitrary)]
    pub enum ArbitraryDisposition {
        InService,
        Expunged,
    }

    impl From<ArbitraryDisposition> for BlueprintDatasetDisposition {
        fn from(value: ArbitraryDisposition) -> Self {
            match value {
                ArbitraryDisposition::InService => Self::InService,
                ArbitraryDisposition::Expunged => Self::Expunged,
            }
        }
    }

    #[derive(Debug, Clone, Copy, Arbitrary)]
    pub struct DatasetPrep {
        pub disposition: ArbitraryDisposition,
        pub in_inventory: bool,
        pub in_database: bool,
    }

    pub fn usize_to_id<T: TypedUuidKind>(n: usize) -> TypedUuid<T> {
        let untyped = Uuid::from_u128(n.try_into().unwrap());
        TypedUuid::from_untyped_uuid(untyped)
    }
}
