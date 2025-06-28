// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for automatic update planning.

use crate::app::background::BackgroundTask;
use chrono::Utc;
use futures::future::BoxFuture;
use nexus_auth::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_reconfigurator_planning::planner::Planner;
use nexus_reconfigurator_preparation::PlanningInputFromDb;
use nexus_types::deployment::ReconfiguratorChickenSwitches;
use nexus_types::deployment::{Blueprint, BlueprintTarget};
use nexus_types::internal_api::background::BlueprintPlannerStatus;
use omicron_common::api::external::LookupType;
use omicron_uuid_kinds::CollectionUuid;
use omicron_uuid_kinds::GenericUuid as _;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::watch::{self, Receiver, Sender};

/// Background task that runs the update planner.
pub struct BlueprintPlanner {
    datastore: Arc<DataStore>,
    rx_chicken_switches: Receiver<Option<ReconfiguratorChickenSwitches>>,
    rx_inventory: Receiver<Option<CollectionUuid>>,
    rx_blueprint: Receiver<Option<Arc<(BlueprintTarget, Blueprint)>>>,
    tx_blueprint: Sender<Option<Arc<(BlueprintTarget, Blueprint)>>>,
}

impl BlueprintPlanner {
    pub fn new(
        datastore: Arc<DataStore>,
        rx_chicken_switches: Receiver<Option<ReconfiguratorChickenSwitches>>,
        rx_inventory: Receiver<Option<CollectionUuid>>,
        rx_blueprint: Receiver<Option<Arc<(BlueprintTarget, Blueprint)>>>,
    ) -> Self {
        let (tx_blueprint, _) = watch::channel(None);
        Self {
            datastore,
            rx_chicken_switches,
            rx_inventory,
            rx_blueprint,
            tx_blueprint,
        }
    }

    pub fn watcher(
        &self,
    ) -> watch::Receiver<Option<Arc<(BlueprintTarget, Blueprint)>>> {
        self.tx_blueprint.subscribe()
    }

    /// Run a planning iteration to generate a new blueprint.
    /// If it is different from the current target blueprint,
    /// save it and make it the current target.
    pub async fn plan(&mut self, opctx: &OpContext) -> BlueprintPlannerStatus {
        let switches = self.rx_chicken_switches.borrow_and_update().clone();
        if switches.is_none_or(|s| !s.planner_enabled) {
            debug!(&opctx.log, "blueprint planning disabled, doing nothing");
            return BlueprintPlannerStatus::Disabled;
        }

        // Get the current target blueprint to use as a parent.
        // Cloned so that we don't block the channel.
        let Some(loaded) = self.rx_blueprint.borrow_and_update().clone() else {
            warn!(
                &opctx.log,
                "blueprint planning skipped";
                "reason" => "no target blueprint loaded"
            );
            return BlueprintPlannerStatus::Error(String::from(
                "no target blueprint to use as parent for planning",
            ));
        };
        let (target, parent) = &*loaded;
        let parent_blueprint_id = parent.id;

        // Get the inventory most recently seen by the collection
        // background task. The value is `Copy`, so with the deref
        // we don't block the channel.
        let Some(collection_id) = *self.rx_inventory.borrow_and_update() else {
            warn!(
                &opctx.log,
                "blueprint planning skipped";
                "reason" => "no inventory collection available"
            );
            return BlueprintPlannerStatus::Error(String::from(
                "no inventory collection available",
            ));
        };
        let collection = match self
            .datastore
            .inventory_collection_read(opctx, collection_id)
            .await
        {
            Ok(collection) => collection,
            Err(error) => {
                error!(
                    &opctx.log,
                    "can't read inventory collection";
                    "collection_id" => %collection_id,
                    "error" => %error,
                );
                return BlueprintPlannerStatus::Error(format!(
                    "can't read inventory collection {}: {}",
                    collection_id, error
                ));
            }
        };

        // Assemble the planning context.
        let input =
            match PlanningInputFromDb::assemble(opctx, &self.datastore).await {
                Ok(input) => input,
                Err(error) => {
                    error!(
                        &opctx.log,
                        "can't assemble planning input";
                        "error" => %error,
                    );
                    return BlueprintPlannerStatus::Error(format!(
                        "can't assemble planning input: {error}"
                    ));
                }
            };

        // Generate a new blueprint.
        let planner = match Planner::new_based_on(
            opctx.log.clone(),
            &parent,
            &input,
            "blueprint_planner",
            &collection,
        ) {
            Ok(planner) => planner,
            Err(error) => {
                error!(
                    &opctx.log,
                    "can't make planner";
                    "error" => %error,
                    "parent_blueprint_id" => %parent_blueprint_id,
                );
                return BlueprintPlannerStatus::Error(format!(
                    "can't make planner based on {}: {}",
                    parent_blueprint_id, error
                ));
            }
        };
        let blueprint = match planner.plan() {
            Ok(blueprint) => blueprint,
            Err(error) => {
                error!(&opctx.log, "can't plan: {error}");
                return BlueprintPlannerStatus::Error(format!(
                    "can't plan: {error}"
                ));
            }
        };

        // Compare the new blueprint to its parent.
        let summary = blueprint.diff_since_blueprint(&parent);
        if !summary.has_changes() {
            // Blueprint is unchanged, do nothing.
            info!(
                &opctx.log,
                "blueprint unchanged from current target";
                "parent_blueprint_id" => %parent_blueprint_id,
            );
            return BlueprintPlannerStatus::Unchanged { parent_blueprint_id };
        }

        // We have a fresh blueprint; save it.
        let blueprint_id = blueprint.id;
        info!(
            &opctx.log,
            "planning produced new blueprint";
            "parent_blueprint_id" => %parent_blueprint_id,
            "blueprint_id" => %blueprint_id,
        );
        match self.datastore.blueprint_insert(opctx, &blueprint).await {
            Ok(()) => (),
            Err(error) => {
                error!(
                    &opctx.log,
                    "can't save blueprint";
                    "error" => %error,
                    "blueprint_id" => %blueprint_id,
                );
                return BlueprintPlannerStatus::Error(format!(
                    "can't save blueprint {}: {}",
                    blueprint_id, error
                ));
            }
        }

        // Try to make it the current target.
        let target = BlueprintTarget {
            target_id: blueprint_id,
            enabled: target.enabled, // copy previous `enabled` flag
            time_made_target: Utc::now(),
        };
        match self.datastore.blueprint_target_set_current(opctx, target).await {
            Ok(()) => (),
            Err(error) => {
                warn!(
                    &opctx.log,
                    "can't make blueprint the current target, deleting it";
                    "error" => %error,
                    "blueprint_id" => %blueprint_id
                );
                let blueprint_id = blueprint_id.into_untyped_uuid();
                let authz_blueprint = authz::Blueprint::new(
                    authz::FLEET,
                    blueprint_id,
                    LookupType::ById(blueprint_id),
                );
                match self
                    .datastore
                    .blueprint_delete(&opctx, &authz_blueprint)
                    .await
                {
                    Ok(()) => (),
                    Err(error) => {
                        error!(
                            &opctx.log,
                            "can't delete blueprint, leaking it";
                            "error" => %error,
                            "blueprint_id" => %blueprint_id,
                        );
                    }
                }
                return BlueprintPlannerStatus::Planned {
                    parent_blueprint_id,
                    error: format!("{error}"),
                };
            }
        }

        // We have a new target!
        self.tx_blueprint.send_replace(Some(Arc::new((target, blueprint))));
        BlueprintPlannerStatus::Targeted { parent_blueprint_id, blueprint_id }
    }
}

impl BackgroundTask for BlueprintPlanner {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        Box::pin(async move { json!(self.plan(opctx).await) })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::app::background::tasks::blueprint_load::TargetBlueprintLoader;
    use crate::app::background::tasks::inventory_collection::InventoryCollector;
    use nexus_inventory::now_db_precision;
    use nexus_test_utils_macros::nexus_test;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    #[nexus_test(server = crate::Server)]
    async fn test_blueprint_planner(cptestctx: &ControlPlaneTestContext) {
        // Set up the test context.
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        // Spin up the blueprint loader background task.
        let mut loader = TargetBlueprintLoader::new(datastore.clone());
        let mut rx_loader = loader.watcher();
        loader.activate(&opctx).await;
        let (_initial_target, initial_blueprint) = &*rx_loader
            .borrow_and_update()
            .clone()
            .expect("no initial blueprint");

        // Spin up the inventory collector background task.
        let resolver = internal_dns_resolver::Resolver::new_from_addrs(
            cptestctx.logctx.log.clone(),
            &[cptestctx.internal_dns.dns_server.local_address()],
        )
        .unwrap();
        let mut collector = InventoryCollector::new(
            datastore.clone(),
            resolver.clone(),
            "test_planner",
            1,
            false,
        );
        let rx_collector = collector.watcher();
        collector.activate(&opctx).await;

        // Enable the planner
        let (_tx, chicken_switches_collector_rx) =
            watch::channel(Some(ReconfiguratorChickenSwitches {
                version: 1,
                planner_enabled: true,
                time_modified: now_db_precision(),
            }));

        // Finally, spin up the planner background task.
        let mut planner = BlueprintPlanner::new(
            datastore.clone(),
            chicken_switches_collector_rx,
            rx_collector,
            rx_loader.clone(),
        );
        let _rx_planner = planner.watcher();

        // On activation, the planner should run successfully and generate
        // a new target blueprint.
        let status = serde_json::from_value::<BlueprintPlannerStatus>(
            planner.activate(&opctx).await,
        )
        .unwrap();
        let blueprint_id = match status {
            BlueprintPlannerStatus::Targeted {
                parent_blueprint_id,
                blueprint_id,
            } if parent_blueprint_id == initial_blueprint.id
                && blueprint_id != initial_blueprint.id =>
            {
                blueprint_id
            }
            _ => panic!("expected new target blueprint"),
        };

        // Load and check the new target blueprint.
        loader.activate(&opctx).await;
        let (target, blueprint) = &*rx_loader
            .borrow_and_update()
            .clone()
            .expect("failed to load blueprint");
        assert_eq!(target.target_id, blueprint.id);
        assert_eq!(target.target_id, blueprint_id);
        assert!(
            blueprint.diff_since_blueprint(initial_blueprint).has_changes()
        );

        // Planning again should not change the plan.
        let status = serde_json::from_value::<BlueprintPlannerStatus>(
            planner.activate(&opctx).await,
        )
        .unwrap();
        assert_eq!(
            status,
            BlueprintPlannerStatus::Unchanged {
                parent_blueprint_id: blueprint_id,
            }
        );
    }
}
