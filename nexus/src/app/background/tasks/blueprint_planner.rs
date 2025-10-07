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
use nexus_db_queries::db::datastore::BlueprintLimitReachedOutput;
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
use slog_error_chain::InlineErrorChain;
use std::sync::Arc;
use tokio::sync::watch::{self, Receiver, Sender};

/// Background task that runs the update planner.
pub struct BlueprintPlanner {
    datastore: Arc<DataStore>,
    rx_config: Receiver<ReconfiguratorConfigLoaderState>,
    rx_inventory: Receiver<Option<CollectionUuid>>,
    rx_blueprint: Receiver<Option<Arc<(BlueprintTarget, Blueprint)>>>,
    tx_blueprint: Sender<Option<Arc<(BlueprintTarget, Blueprint)>>>,
    blueprint_limit: u64,
}

/// The default number of blueprints, beyond which the auto-planner will stop
/// generating new blueprints.
///
/// This limit is chosen based on the desire to have up to 5 updates worth of
/// blueprints stored in the system before we need to start archiving them.
///
/// * For an update, each individual component update gets a fresh blueprint.
/// * There are roughly 15 components per sled, and on a full rack (32 sleds)
///   that's around 450 components.
/// * With some more for extra services and minor flakiness (e.g. dueling
///   Nexuses wanting to overwrite each other's blueprints until they
///   eventually converge onto the same blueprint), a round figure is 500.
/// * For 5 updates, we would have 2500 blueprints.
/// * A safety factor of 2 results in 5000 blueprints.
const DEFAULT_BLUEPRINT_LIMIT: u64 = 5000;

impl BlueprintPlanner {
    pub fn new(
        datastore: Arc<DataStore>,
        rx_config: Receiver<ReconfiguratorConfigLoaderState>,
        rx_inventory: Receiver<Option<CollectionUuid>>,
        rx_blueprint: Receiver<Option<Arc<(BlueprintTarget, Blueprint)>>>,
    ) -> Self {
        let (tx_blueprint, _) = watch::channel(None);
        Self {
            datastore,
            rx_config,
            rx_inventory,
            rx_blueprint,
            tx_blueprint,
            blueprint_limit: DEFAULT_BLUEPRINT_LIMIT,
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

        // Check if the blueprint limit has been reached.
        //
        // Do this after the planning report is generated so that we can include
        // the report in the LimitReached case.
        //
        // Do this *before* comparing the new blueprint to its parent, since we
        // want to return this error even if the blueprint is unchanged (the
        // next time there's a change we'll return an error anyway -- the limit
        // being reached is bad whether we'd store the next blueprint or not).
        if let Some(status) =
            self.check_blueprint_limit_reached(opctx, &report).await
        {
            return status;
        }

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
                    report,
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
                    report,
                };
            }
        }

        // We have a new target!

        self.tx_blueprint.send_replace(Some(Arc::new((target, blueprint))));
        BlueprintPlannerStatus::Targeted {
            parent_blueprint_id,
            blueprint_id,
            report,
        }
    }

    async fn check_blueprint_limit_reached(
        &self,
        opctx: &OpContext,
        report: &Arc<PlanningReport>,
    ) -> Option<BlueprintPlannerStatus> {
        let blueprint_count = match self
            .datastore
            .check_blueprint_limit_reached(opctx, self.blueprint_limit)
            .await
        {
            Ok(BlueprintLimitReachedOutput::Yes) => {
                error!(
                    &opctx.log,
                    "blueprint count at or over limit, not running auto-planner";
                    "limit" => self.blueprint_limit,
                );
                return Some(BlueprintPlannerStatus::LimitReached {
                    limit: self.blueprint_limit,
                    report: report.clone(),
                });
            }
            Ok(BlueprintLimitReachedOutput::No { count }) => count,
            Err(error) => {
                error!(
                    &opctx.log,
                    "can't load blueprint count";
                    "error" => InlineErrorChain::new(&error),
                );
                return Some(BlueprintPlannerStatus::Error(format!(
                    "can't load blueprint count: {}",
                    InlineErrorChain::new(&error)
                )));
            }
        };

        let usage_percent =
            blueprint_count.saturating_mul(100) / self.blueprint_limit;

        match usage_percent {
            0..=59 => {
                debug!(
                    &opctx.log,
                    "blueprint count under limit, proceeding with planning";
                    "limit" => self.blueprint_limit,
                    "count" => blueprint_count,
                    "usage_percent" => usage_percent,
                );
            }
            60..=79 => {
                info!(
                    &opctx.log,
                    "blueprint count above 60% of limit, proceeding with planning \
                     (will stop autoplanning if limit is reached)";
                    "limit" => self.blueprint_limit,
                    "count" => blueprint_count,
                    "usage_percent" => usage_percent,
                );
            }
            80.. => {
                warn!(
                    &opctx.log,
                    "blueprint count above 80% of limit, proceeding with planning \
                     (will stop autoplanning if limit is reached)";
                    "limit" => self.blueprint_limit,
                    "count" => blueprint_count,
                    "usage_percent" => usage_percent,
                );
            }
        }

        None
    }
}

impl BackgroundTask for BlueprintPlanner {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        Box::pin(async move {
            let status = self.plan(opctx).await;
            match serde_json::to_value(status) {
                Ok(val) => val,
                Err(err) => json!({
                    "error": format!("could not serialize task status: {}",
                                     InlineErrorChain::new(&err)),
                }),
            }
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::app::background::tasks::blueprint_execution::BlueprintExecutor;
    use crate::app::background::tasks::blueprint_load::TargetBlueprintLoader;
    use crate::app::background::tasks::inventory_collection::InventoryCollector;
    use crate::app::{background::Activator, quiesce::NexusQuiesceHandle};
    use assert_matches::assert_matches;
    use nexus_inventory::now_db_precision;
    use nexus_reconfigurator_planning::blueprint_builder::BlueprintBuilder;
    use nexus_test_utils::db::TestDatabase;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::deployment::{
        PendingMgsUpdates, PlannerConfig, ReconfiguratorConfig,
        ReconfiguratorConfigView,
    };
    use omicron_test_utils::dev;
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
                    planner_config: PlannerConfig::default(),
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
        assert_matches!(
            status,
            BlueprintPlannerStatus::Unchanged {
                parent_blueprint_id,
                report: _,
            } if parent_blueprint_id == parent_blueprint_id
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
        assert_matches!(
            status,
            BlueprintPlannerStatus::Unchanged {
                parent_blueprint_id,
                report: _,
            } if parent_blueprint_id == blueprint_id
        );
    }

    /// Test that the blueprint autoplanner verifies that the blueprint limit is
    /// not exceeded.
    ///
    /// This is a tokio::test rather than a nexus_test to avoid background tasks
    /// interfering with the test.
    #[tokio::test]
    async fn test_blueprint_planner_limit() {
        // Setup
        let logctx = dev::test_setup_log("test_blueprint_planner_limit");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let opctx = db.opctx();
        let datastore = db.datastore();

        // Create a large number of blueprints (49), which we'll use to test the
        // limit (see below).
        for i in 0..49 {
            let blueprint = BlueprintBuilder::build_empty_with_sleds(
                std::iter::empty(),
                &format!("test_blueprint_planner_limit blueprint {}", i),
            );
            datastore
                .blueprint_insert(&opctx, &blueprint)
                .await
                .unwrap_or_else(|e| {
                    panic!(
                        "failed to insert blueprint at iteration {i}: {}",
                        InlineErrorChain::new(&e)
                    );
                });
        }

        // Enable the planner.
        let (_tx, rx_config_loader) = watch::channel(
            ReconfiguratorConfigLoaderState::Loaded(ReconfiguratorConfigView {
                version: 1,
                config: ReconfiguratorConfig {
                    planner_enabled: true,
                    planner_config: PlannerConfig::default(),
                },
                time_modified: now_db_precision(),
            }),
        );
        // The inventory and blueprint channels don't need to be Some because we
        // don't run through the whole planner -- we just call
        // check_blueprint_limit_reached.
        let (_tx_inventory, rx_inventory) = watch::channel(None);
        let (_tx_blueprint, rx_blueprint) = watch::channel(None);

        let mut planner = BlueprintPlanner::new(
            datastore.clone(),
            rx_config_loader,
            rx_inventory,
            rx_blueprint,
        );

        // This limit matches the loop above.
        planner.blueprint_limit = 50;
        let _rx_planner = planner.watcher();

        // This should work since there are 49 blueprints, which is one less
        // than the limit (50).
        let report = Arc::new(PlanningReport::new());
        if let Some(status) =
            planner.check_blueprint_limit_reached(opctx, &report).await
        {
            panic!(
                "check_blueprint_limit_reached should have returned None, \
                 but returned Some({:?})",
                status
            );
        }

        // Insert one more blueprint, pushing the number of blueprints to the
        // limit (50).
        let blueprint = BlueprintBuilder::build_empty_with_sleds(
            std::iter::empty(),
            "test_blueprint_planner_limit 50th blueprint",
        );
        datastore.blueprint_insert(&opctx, &blueprint).await.unwrap_or_else(
            |e| {
                panic!(
                    "failed to insert 50th blueprint: {}",
                    InlineErrorChain::new(&e)
                );
            },
        );

        // Since blueprint 50 was created, check_blueprint_limit_reached should
        // fail with LimitExceeded.
        let status = planner
            .check_blueprint_limit_reached(&opctx, &report)
            .await
            .expect("check_blueprint_limit_reached should return LimitReached");
        eprintln!("status after second check: {:?}", status);
        assert_eq!(
            status,
            BlueprintPlannerStatus::LimitReached {
                limit: 50,
                report: report.clone(),
            }
        );

        // But manual planning should continue to work.
        let blueprint = BlueprintBuilder::build_empty_with_sleds(
            std::iter::empty(),
            "test_blueprint_planner_limit 51st blueprint",
        );
        datastore.blueprint_insert(&opctx, &blueprint).await.unwrap_or_else(
            |e| {
                panic!(
                    "manual planning should continue to work even \
                     though the limit was exceeded, but it failed: {}",
                    InlineErrorChain::new(&e)
                );
            },
        );
        let status = planner
            .check_blueprint_limit_reached(&opctx, &report)
            .await
            .expect("check_blueprint_limit_reached should return LimitReached");
        eprintln!("status after third check: {:?}", status);
        assert_eq!(
            status,
            BlueprintPlannerStatus::LimitReached {
                limit: 50,
                report: report.clone(),
            }
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
