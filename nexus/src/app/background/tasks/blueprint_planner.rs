// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for automatic update planning.

use super::reconfigurator_config::ReconfiguratorConfigLoaderState;
use crate::app::background::BackgroundTask;
use chrono::Utc;
use futures::future::BoxFuture;
use nexus_auth::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_reconfigurator_planning::planner::Planner;
use nexus_reconfigurator_planning::planner::PlannerRng;
use nexus_reconfigurator_preparation::PlanningInputFromDb;
use nexus_types::deployment::BlueprintSource;
use nexus_types::deployment::PlanningReport;
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
    rx_config: Receiver<ReconfiguratorConfigLoaderState>,
    rx_inventory: Receiver<Option<CollectionUuid>>,
    rx_blueprint: Receiver<Option<Arc<(BlueprintTarget, Blueprint)>>>,
    tx_blueprint: Sender<Option<Arc<(BlueprintTarget, Blueprint)>>>,
}

impl BlueprintPlanner {
    pub fn new(
        datastore: Arc<DataStore>,
        rx_config: Receiver<ReconfiguratorConfigLoaderState>,
        rx_inventory: Receiver<Option<CollectionUuid>>,
        rx_blueprint: Receiver<Option<Arc<(BlueprintTarget, Blueprint)>>>,
    ) -> Self {
        let (tx_blueprint, _) = watch::channel(None);
        Self { datastore, rx_config, rx_inventory, rx_blueprint, tx_blueprint }
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
        // Refuse to run if we haven't had a chance to load our config from the
        // database yet. (There might not be a config, which is fine! But the
        // loading task needs to have a chance to check.)
        let config = match &*self.rx_config.borrow_and_update() {
            ReconfiguratorConfigLoaderState::NotYetLoaded => {
                debug!(
                    opctx.log,
                    "reconfigurator config not yet loaded; doing nothing"
                );
                return BlueprintPlannerStatus::Disabled;
            }
            ReconfiguratorConfigLoaderState::Loaded(config) => config.clone(),
        };
        if !config.config.planner_enabled {
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
        let input = match PlanningInputFromDb::assemble(
            opctx,
            &self.datastore,
            config.config.planner_config,
        )
        .await
        {
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
            PlannerRng::from_entropy(),
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
        {
            let summary = blueprint.diff_since_blueprint(&parent);
            if !summary.has_changes() {
                // Blueprint is unchanged, do nothing.
                info!(
                    &opctx.log,
                    "blueprint unchanged from current target";
                    "parent_blueprint_id" => %parent_blueprint_id,
                );
                return BlueprintPlannerStatus::Unchanged {
                    parent_blueprint_id,
                };
            }
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

        // We just ran the planner, so we should always get its report. This
        // output is for debugging only, though, so just make an empty one in
        // the unreachable arms.
        let report = match &blueprint.source {
            BlueprintSource::Planner(report) => Arc::clone(report),
            BlueprintSource::Rss
            | BlueprintSource::PlannerLoadedFromDatabase
            | BlueprintSource::ReconfiguratorCliEdit
            | BlueprintSource::Test => {
                warn!(
                    &opctx.log,
                    "ran planner, but got unexpected blueprint source; \
                     generating an empty planning report";
                    "source" => ?&blueprint.source,
                );
                Arc::new(PlanningReport::new())
            }
        };
        self.tx_blueprint.send_replace(Some(Arc::new((target, blueprint))));
        BlueprintPlannerStatus::Targeted {
            parent_blueprint_id,
            blueprint_id,
            report,
        }
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
    use crate::app::background::tasks::blueprint_execution::BlueprintExecutor;
    use crate::app::background::tasks::blueprint_load::TargetBlueprintLoader;
    use crate::app::background::tasks::inventory_collection::InventoryCollector;
    use crate::app::{background::Activator, quiesce::NexusQuiesceHandle};
    use nexus_inventory::now_db_precision;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::deployment::{
        PendingMgsUpdates, PlannerConfig, ReconfiguratorConfig,
        ReconfiguratorConfigView,
    };
    use omicron_uuid_kinds::OmicronZoneUuid;
    use std::collections::BTreeMap;

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
        let (tx_loader, _) = watch::channel(None);
        let mut loader =
            TargetBlueprintLoader::new(datastore.clone(), tx_loader);
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
        .expect("can't start resolver");
        let mut collector = InventoryCollector::new(
            &opctx,
            datastore.clone(),
            resolver.clone(),
            "test_planner",
            1,
            false,
        );
        let rx_collector = collector.watcher();
        collector.activate(&opctx).await;

        // Enable the planner
        let (_tx, rx_config_loader) = watch::channel(
            ReconfiguratorConfigLoaderState::Loaded(ReconfiguratorConfigView {
                version: 1,
                config: ReconfiguratorConfig {
                    planner_enabled: true,
                    planner_config: PlannerConfig {
                        // Set this config to true because we'd like to test
                        // adding zones even if no target release is set. In the
                        // future, we'll allow adding zones if no target release
                        // has ever been set, in which case we can go back to
                        // setting this field to false.
                        add_zones_with_mupdate_override: true,
                    },
                },
                time_modified: now_db_precision(),
            }),
        );

        // Finally, spin up the planner background task.
        let mut planner = BlueprintPlanner::new(
            datastore.clone(),
            rx_config_loader,
            rx_collector,
            rx_loader.clone(),
        );
        let _rx_planner = planner.watcher();

        // On activation, the planner should run successfully and generate
        // a new target blueprint.
        let status = serde_json::from_value::<BlueprintPlannerStatus>(
            planner.activate(&opctx).await,
        )
        .expect("can't activate planner");
        let blueprint_id = match status {
            BlueprintPlannerStatus::Targeted {
                parent_blueprint_id,
                blueprint_id,
                report: _,
            } if parent_blueprint_id == initial_blueprint.id
                && blueprint_id != initial_blueprint.id =>
            {
                blueprint_id
            }
            other => panic!("expected new target blueprint, found {other:?}"),
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

        // Planning again should not change the plan, because nothing has changed.
        let status = serde_json::from_value::<BlueprintPlannerStatus>(
            planner.activate(&opctx).await,
        )
        .expect("can't re-activate planner");
        assert_eq!(
            status,
            BlueprintPlannerStatus::Unchanged {
                parent_blueprint_id: blueprint_id,
            }
        );

        // Enable execution.
        let mut target = *target;
        target.enabled = true;
        datastore
            .blueprint_target_set_current_enabled(&opctx, target)
            .await
            .expect("can't enable execution");

        // Ping the loader again so it gets the updated target.
        loader.activate(&opctx).await;
        let (target, blueprint) = &*rx_loader
            .borrow_and_update()
            .clone()
            .expect("failed to re-load blueprint");
        assert_eq!(target.target_id, blueprint.id);
        assert_eq!(target.target_id, blueprint_id);
        assert!(
            blueprint.diff_since_blueprint(initial_blueprint).has_changes()
        );

        // Trigger an inventory collection.
        collector.activate(&opctx).await;

        // Execute the plan.
        let (dummy_tx, _dummy_rx) = watch::channel(PendingMgsUpdates::new());
        let mut executor = BlueprintExecutor::new(
            datastore.clone(),
            resolver.clone(),
            rx_loader.clone(),
            OmicronZoneUuid::new_v4(),
            Activator::new(),
            dummy_tx,
            NexusQuiesceHandle::new(
                datastore.clone(),
                OmicronZoneUuid::new_v4(),
                rx_loader.clone(),
                opctx.child(BTreeMap::new()),
            ),
        );
        let value = executor.activate(&opctx).await;
        let value = value.as_object().expect("response is not a JSON object");
        assert_eq!(value["target_id"], blueprint.id.to_string());
        assert!(value["enabled"].as_bool().expect("enabled should be boolean"));
        assert!(value["execution_error"].is_null());
        assert!(
            !value["event_report"]
                .as_object()
                .expect("event report should be an object")["Ok"]
                .as_object()
                .expect("event report is not Ok")["step_events"]
                .as_array()
                .expect("steps should be an array")
                .is_empty()
        );

        // Planning again should not change the plan, because the execution
        // is all fake.
        let status = serde_json::from_value::<BlueprintPlannerStatus>(
            planner.activate(&opctx).await,
        )
        .expect("can't re-activate planner");
        assert_eq!(
            status,
            BlueprintPlannerStatus::Unchanged {
                parent_blueprint_id: blueprint_id,
            }
        );
    }
}
