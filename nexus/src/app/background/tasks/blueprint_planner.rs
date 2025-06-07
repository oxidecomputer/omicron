// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for automatic update planning.

use crate::app::background::BackgroundTask;
use chrono::Utc;
use futures::future::BoxFuture;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_reconfigurator_planning::planner::Planner;
use nexus_reconfigurator_preparation::PlanningInputFromDb;
use nexus_types::deployment::{Blueprint, BlueprintTarget};
use nexus_types::internal_api::background::BlueprintPlannerStatus;
use omicron_uuid_kinds::CollectionUuid;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::watch::{self, Receiver, Sender};

/// Background task that runs the update planner.
pub struct BlueprintPlanner {
    datastore: Arc<DataStore>,
    disabled: bool,
    rx_inventory: Receiver<Option<CollectionUuid>>,
    rx_blueprint: Receiver<Option<Arc<(BlueprintTarget, Blueprint)>>>,
    tx_blueprint: Sender<Option<Arc<(BlueprintTarget, Blueprint)>>>,
}

impl BlueprintPlanner {
    pub fn new(
        datastore: Arc<DataStore>,
        disabled: bool,
        rx_inventory: Receiver<Option<CollectionUuid>>,
        rx_blueprint: Receiver<Option<Arc<(BlueprintTarget, Blueprint)>>>,
    ) -> Self {
        let (tx_blueprint, _) = watch::channel(None);
        Self { datastore, disabled, rx_inventory, rx_blueprint, tx_blueprint }
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
        let mut status = BlueprintPlannerStatus::default();
        if self.disabled {
            debug!(&opctx.log, "blueprint planning disabled, doing nothing");
            status.disabled = true;
            return status;
        }

        // Get the current target blueprint to use as a parent.
        // Cloned so that we don't block the channel.
        let Some(loaded) = self.rx_blueprint.borrow_and_update().clone() else {
            warn!(
                &opctx.log,
                "blueprint planning skipped";
                "reason" => "no target blueprint loaded"
            );
            status.error = Some(String::from(
                "no target blueprint to use as parent for planning",
            ));
            return status;
        };
        let (target, parent) = &*loaded;

        // Get the inventory most recently seen by the collection
        // background task. The value is `Copy`, so with the deref
        // we don't block the channel.
        let Some(collection_id) = *self.rx_inventory.borrow_and_update() else {
            warn!(
                &opctx.log,
                "blueprint planning skipped";
                "reason" => "no inventory collection available"
            );
            status.error =
                Some(String::from("no inventory collection available"));
            return status;
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
                status.error = Some(format!(
                    "can't read inventory collection {}: {}",
                    collection_id, error
                ));
                return status;
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
                    status.error =
                        Some(format!("can't assemble planning input: {error}"));
                    return status;
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
                    "parent_blueprint_id" => %parent.id,
                );
                status.error = Some(format!(
                    "can't make planner based on {}: {}",
                    parent.id, error
                ));
                return status;
            }
        };
        let blueprint = match planner.plan() {
            Ok(blueprint) => blueprint,
            Err(error) => {
                error!(&opctx.log, "can't plan: {error}");
                status.error = Some(format!("can't plan: {error}"));
                return status;
            }
        };

        // Compare the new blueprint to its parent.
        let summary = blueprint.diff_since_blueprint(&parent);
        if summary.has_changes() {
            info!(
                &opctx.log,
                "planning produced new blueprint";
                "parent_blueprint_id" => %parent.id,
                "blueprint_id" => %blueprint.id,
            );

            // Save it.
            match self.datastore.blueprint_insert(opctx, &blueprint).await {
                Ok(()) => (),
                Err(error) => {
                    error!(
                        &opctx.log,
                        "can't save blueprint";
                        "error" => %error,
                        "blueprint_id" => %blueprint.id,
                    );
                    status.error = Some(format!(
                        "can't save blueprint {}: {}",
                        blueprint.id, error
                    ));
                    return status;
                }
            }

            // Make it the current target.
            let target = BlueprintTarget {
                target_id: blueprint.id,
                enabled: target.enabled,
                time_made_target: Utc::now(),
            };
            match self
                .datastore
                .blueprint_target_set_current(opctx, target)
                .await
            {
                Ok(()) => (),
                Err(error) => {
                    warn!(
                        &opctx.log,
                        "can't make blueprint the current target";
                        "error" => %error,
                        "blueprint_id" => %blueprint.id
                    );
                    status.error = Some(format!(
                        "can't make blueprint {} the current target: {}",
                        blueprint.id, error,
                    ));
                    return status;
                }
            }

            // Notify watchers that we have a new target.
            self.tx_blueprint.send_replace(Some(Arc::new((target, blueprint))));
        } else {
            // Blueprint is unchanged, do nothing.
            info!(
                &opctx.log,
                "blueprint unchanged from current target";
                "parent_blueprint_id" => %parent.id,
            );
            status.unchanged = true;
        }

        status
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
