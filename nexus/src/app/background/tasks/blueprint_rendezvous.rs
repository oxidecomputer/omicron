// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for reconciling blueprints and inventory, updating
//! Reconfigurator rendezvous tables

use crate::app::background::{
    BackgroundTask, tasks::blueprint_load::LoadedTargetBlueprint,
};
use futures::FutureExt;
use futures::future::BoxFuture;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_reconfigurator_rendezvous::reconcile_dataset_rendezvous_tables;
use nexus_reconfigurator_rendezvous::reconcile_sled_blueprint_availability;
use nexus_types::internal_api::background::BlueprintRendezvousStatus;
use nexus_types::internal_api::background::DatasetRendezvousOutcome;
use nexus_types::internal_api::background::SledBlueprintAvailabilityRendezvousOutcome;
use nexus_types::inventory::Collection;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::watch;

/// Background task that takes a `Blueprint` and an inventory `Collection`
/// and updates any rendezvous tables to track resources under Reconfigurator's
/// control for other parts of Nexus to consume.
pub struct BlueprintRendezvous {
    datastore: Arc<DataStore>,
    rx_blueprint: watch::Receiver<Option<LoadedTargetBlueprint>>,
    rx_inventory: watch::Receiver<Option<Arc<Collection>>>,
}

impl BlueprintRendezvous {
    pub fn new(
        datastore: Arc<DataStore>,
        rx_blueprint: watch::Receiver<Option<LoadedTargetBlueprint>>,
        rx_inventory: watch::Receiver<Option<Arc<Collection>>>,
    ) -> Self {
        Self { datastore, rx_blueprint, rx_inventory }
    }

    /// Implementation for `BackgroundTask::activate` for `BlueprintRendezvous`,
    /// added here to produce better compile errors.
    ///
    /// The presence of `boxed()` in `BackgroundTask::activate` has caused some
    /// confusion with compilation errors in the past. So separate this method
    /// out.
    async fn activate_impl(&mut self, opctx: &OpContext) -> serde_json::Value {
        // Get the latest blueprint, cloning to prevent holding a read lock
        // on the watch.
        let update = self.rx_blueprint.borrow_and_update().clone();
        let Some(LoadedTargetBlueprint { blueprint, target: _ }) = update
        else {
            warn!(
                &opctx.log, "Blueprint rendezvous: skipped";
                "reason" => "no blueprint",
            );
            return json!({"error": "no blueprint" });
        };

        // Reconcile sled availability -- this can be done without an inventory
        // collection available.
        match reconcile_sled_blueprint_availability(
            opctx,
            &self.datastore,
            &blueprint,
        )
        .await
        {
            Ok(stats) => {
                SledBlueprintAvailabilityRendezvousOutcome::Reconciled(stats)
            }
            Err(err) => {
                error!(
                    &opctx.log,
                    "Blueprint rendezvous: sled availability reconciliation \
                     failed";
                    "blueprint_id" => %blueprint.id,
                    "error" => format!("{err:#}"),
                );
                SledBlueprintAvailabilityRendezvousOutcome::Error(format!(
                    "{err:#}"
                ))
            }
        };

        // Get the inventory most recently seen by the inventory loader
        // background task. We clone the Arc to avoid keeping the channel locked
        // for the rest of our execution.
        let inventory =
            self.rx_inventory.borrow_and_update().as_ref().map(Arc::clone);
        let datasets = match inventory {
            None => {
                warn!(
                    &opctx.log,
                    "Blueprint rendezvous: skipped dataset reconciliation";
                    "reason" => "no inventory collection",
                );
                DatasetRendezvousOutcome::NoInventoryCollection
            }
            Some(collection) => {
                match reconcile_dataset_rendezvous_tables(
                    opctx,
                    &self.datastore,
                    &blueprint,
                    &collection,
                )
                .await
                {
                    Ok(stats) => DatasetRendezvousOutcome::Reconciled {
                        inventory_collection_id: collection.id,
                        stats,
                    },
                    Err(err) => {
                        error!(
                            &opctx.log,
                            "Blueprint rendezvous: dataset reconciliation \
                             failed";
                            "blueprint_id" => %blueprint.id,
                            "inventory_collection_id" => %collection.id,
                            "error" => format!("{err:#}"),
                        );
                        DatasetRendezvousOutcome::Error {
                            inventory_collection_id: collection.id,
                            error: format!("{err:#}"),
                        }
                    }
                }
            }
        };

        json!(BlueprintRendezvousStatus {
            blueprint_id: blueprint.id,
            sled_blueprint_availability,
            datasets,
        })
    }
}

impl BackgroundTask for BlueprintRendezvous {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        self.activate_impl(opctx).boxed()
    }
}
