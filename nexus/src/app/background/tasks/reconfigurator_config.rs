// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Runtime configuration for reconfigurator

use crate::app::background::BackgroundTask;
use anyhow::Context;
use futures::FutureExt;
use futures::future::BoxFuture;
use nexus_auth::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::ReconfiguratorConfigView;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::watch;

/// Enum that allows downstream tasks to know whether this task has had a chance
/// to read the current config from the database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconfiguratorConfigLoaderState {
    NotYetLoaded,
    Loaded(ReconfiguratorConfigView),
}

/// Background task that tracks reconfigurator config from the DB
pub struct ReconfiguratorConfigLoader {
    datastore: Arc<DataStore>,
    tx: watch::Sender<ReconfiguratorConfigLoaderState>,
}

impl ReconfiguratorConfigLoader {
    pub fn new(datastore: Arc<DataStore>) -> Self {
        let (tx, _rx) =
            watch::channel(ReconfiguratorConfigLoaderState::NotYetLoaded);
        Self { datastore, tx }
    }

    pub fn watcher(&self) -> watch::Receiver<ReconfiguratorConfigLoaderState> {
        self.tx.subscribe()
    }
}

impl BackgroundTask for ReconfiguratorConfigLoader {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            match self
                .datastore
                .reconfigurator_config_get_latest(opctx)
                .await
                .context("failed to load reconfigurator config")
            {
                Err(error) => {
                    let message = format!("{:#}", error);
                    warn!(opctx.log, "reconfigurator config load failed";
                        "error" => message.clone());
                    json!({ "error": message })
                }
                Ok(switches) => {
                    let switches = ReconfiguratorConfigLoaderState::Loaded(
                        switches.unwrap_or_default(),
                    );
                    let updated = self.tx.send_if_modified(|s| {
                        if *s != switches {
                            *s = switches.clone();
                            return true;
                        }
                        false
                    });
                    debug!(
                        opctx.log, "reconfigurator config load complete";
                        "switches" => ?switches,
                    );
                    json!({ "config_updated": updated })
                }
            }
        }
        .boxed()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::app::background::BackgroundTask;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use nexus_lockstep_client::types::LastResult;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::deployment::{
        PlannerConfig, ReconfiguratorConfig, ReconfiguratorConfigParam,
    };
    use nexus_types::internal_api::background::BlueprintPlannerStatus;
    use nexus_types::internal_api::background::TufRepoPrunerStatus;
    use omicron_test_utils::dev::poll::{CondCheckError, wait_for_condition};
    use serde::de::DeserializeOwned;
    use std::time::Duration;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    #[nexus_test(server = crate::Server)]
    async fn test_basic(cptestctx: &ControlPlaneTestContext) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        // `#[nexus_test]` inserts an initial configuration disable planning in
        // general; let's remove that value so we can test from a clean slate.
        //
        // Configuration values are supposed to form a continuous history, so
        // there's no datastore method to delete existing values. We'll go
        // behind its back and delete them directly.
        {
            use nexus_db_schema::schema::reconfigurator_config::dsl;
            let conn = datastore.pool_connection_for_tests().await.unwrap();
            diesel::delete(dsl::reconfigurator_config)
                .execute_async(&*conn)
                .await
                .expect("removed nexus_test default reconfigurator config");
        }

        let mut task = ReconfiguratorConfigLoader::new(datastore.clone());

        // Initial state should be `NotYetLoaded`.
        let mut rx = task.watcher();
        assert_eq!(
            *rx.borrow_and_update(),
            ReconfiguratorConfigLoaderState::NotYetLoaded
        );

        // We haven't inserted anything into the DB, so the initial activation
        // should populate the channel with our default values.
        let default_switches = ReconfiguratorConfigView::default();
        let out = task.activate(&opctx).await;
        assert_eq!(out["config_updated"], true);
        assert!(rx.has_changed().unwrap());
        assert_eq!(
            *rx.borrow_and_update(),
            ReconfiguratorConfigLoaderState::Loaded(default_switches.clone())
        );

        // Insert an initial set of switches.
        let expected_switches = ReconfiguratorConfig {
            planner_enabled: !default_switches.config.planner_enabled,
            planner_config: PlannerConfig::default(),
            tuf_repo_pruner_enabled: true,
        };
        let switches =
            ReconfiguratorConfigParam { version: 1, config: expected_switches };
        datastore
            .reconfigurator_config_insert_latest_version(&opctx, switches)
            .await
            .unwrap();
        let out = task.activate(&opctx).await;
        assert_eq!(out["config_updated"], true);
        assert!(rx.has_changed().unwrap());
        {
            let view = match rx.borrow_and_update().clone() {
                ReconfiguratorConfigLoaderState::NotYetLoaded => {
                    panic!("unexpected value")
                }
                ReconfiguratorConfigLoaderState::Loaded(view) => view,
            };
            assert_eq!(view.version, 1);
            assert_eq!(view.config, expected_switches);
        }

        // Activating again should not change things.
        let out = task.activate(&opctx).await;
        assert_eq!(out["config_updated"], false);
        assert!(!rx.has_changed().unwrap());

        // Insert a new version.
        let expected_switches = ReconfiguratorConfig {
            planner_enabled: !expected_switches.planner_enabled,
            planner_config: PlannerConfig::default(),
            tuf_repo_pruner_enabled: true,
        };
        let switches =
            ReconfiguratorConfigParam { version: 2, config: expected_switches };
        datastore
            .reconfigurator_config_insert_latest_version(&opctx, switches)
            .await
            .unwrap();
        let out = task.activate(&opctx).await;
        assert_eq!(out["config_updated"], true);
        assert!(rx.has_changed().unwrap());
        {
            let view = match rx.borrow_and_update().clone() {
                ReconfiguratorConfigLoaderState::NotYetLoaded => {
                    panic!("unexpected value")
                }
                ReconfiguratorConfigLoaderState::Loaded(view) => view,
            };
            assert_eq!(view.version, 2);
            assert_eq!(view.config, expected_switches);
        }
    }

    /// Tests the actual behavior of enabling and disabling background tasks
    #[nexus_test(server = crate::Server)]
    async fn test_background_tasks_enable_disable(
        cptestctx: &ControlPlaneTestContext,
    ) {
        // This test uses both the Progenitor lockstep client (because that's
        // preferred in modern code) as well as the one hanging directly off the
        // `cptestctx` (which uses ClientTestContext, which predates Progenitor
        // altogether, when calling existing code that uses that client).
        let lockstep_client = cptestctx.lockstep_client();

        // As of this writing, the initial state in the test suite is that the
        // planner is disabled but the TUF repo pruner is enabled.  But we don't
        // want to depend on that.  So the basic plan here will be:
        //
        // - disable both tasks: verify they're disabled
        // - enable both tasks: verify they're enabled
        // - disable both tasks again: verify they're disabled
        //
        // This last step seems redundant but it exercises the transition from
        // enabled -> disabled, which may not have happened if one of the tasks
        // was disabled when we started.

        // Disable both tasks.
        let initial_config_version = lockstep_client
            .reconfigurator_config_show_current()
            .await
            .expect("failed to get current config")
            .version;
        let config_disabled = ReconfiguratorConfig {
            planner_enabled: false,
            planner_config: PlannerConfig::default(),
            tuf_repo_pruner_enabled: false,
        };
        let switches = ReconfiguratorConfigParam {
            version: initial_config_version + 1,
            config: config_disabled,
        };
        lockstep_client
            .reconfigurator_config_set(&switches)
            .await
            .expect("failed to set enabled config");
        eprintln!("disabled both tasks");

        // Wait for both tasks to report being disabled.
        wait_for_task_status(
            &lockstep_client,
            "blueprint_planner",
            &|planner_status: &BlueprintPlannerStatus| {
                matches!(planner_status, BlueprintPlannerStatus::Disabled)
            },
        )
        .await;
        wait_for_task_status(
            &lockstep_client,
            "tuf_repo_pruner",
            &|pruner_status: &TufRepoPrunerStatus| {
                matches!(pruner_status, TufRepoPrunerStatus::Disabled { .. })
            },
        )
        .await;

        // Enable both tasks.
        let config_enabled = ReconfiguratorConfig {
            planner_enabled: true,
            planner_config: PlannerConfig::default(),
            tuf_repo_pruner_enabled: true,
        };
        let switches = ReconfiguratorConfigParam {
            version: initial_config_version + 2,
            config: config_enabled,
        };
        lockstep_client
            .reconfigurator_config_set(&switches)
            .await
            .expect("failed to set enabled config");
        eprintln!("enabled both tasks");

        // Wait for both tasks to report being enabled.
        wait_for_task_status(
            &lockstep_client,
            "blueprint_planner",
            &|planner_status: &BlueprintPlannerStatus| {
                !matches!(planner_status, BlueprintPlannerStatus::Disabled)
            },
        )
        .await;
        wait_for_task_status(
            &lockstep_client,
            "tuf_repo_pruner",
            &|pruner_status: &TufRepoPrunerStatus| {
                !matches!(pruner_status, TufRepoPrunerStatus::Disabled { .. })
            },
        )
        .await;

        // Disable both tasks again.
        let switches = ReconfiguratorConfigParam {
            version: initial_config_version + 3,
            config: config_disabled,
        };
        lockstep_client
            .reconfigurator_config_set(&switches)
            .await
            .expect("failed to set enabled config");
        eprintln!("disabled both tasks again");

        // Wait for both tasks to report being disabled.
        wait_for_task_status(
            &lockstep_client,
            "blueprint_planner",
            &|planner_status: &BlueprintPlannerStatus| {
                matches!(planner_status, BlueprintPlannerStatus::Disabled)
            },
        )
        .await;
        wait_for_task_status(
            &lockstep_client,
            "tuf_repo_pruner",
            &|pruner_status: &TufRepoPrunerStatus| {
                matches!(pruner_status, TufRepoPrunerStatus::Disabled { .. })
            },
        )
        .await;
    }

    async fn wait_for_task_status<T: DeserializeOwned>(
        lockstep_client: &nexus_lockstep_client::Client,
        task_name: &'static str,
        check: &dyn Fn(&T) -> bool,
    ) {
        eprintln!("waiting for {task_name} status");
        wait_for_condition(
            || async {
                let task_status = lockstep_client
                    .bgtask_view(task_name)
                    .await
                    .expect("fetching task status")
                    .into_inner();
                eprintln!("task {} status: {:#?}", task_name, task_status);
                let LastResult::Completed(completed) = task_status.last else {
                    return Err(CondCheckError::<()>::NotYet);
                };

                let status: T = serde_json::from_value(completed.details)
                    .expect("failed to parse task status as JSON");
                if check(&status) {
                    Ok(())
                } else {
                    Err(CondCheckError::NotYet)
                }
            },
            &Duration::from_millis(100),
            &Duration::from_secs(30),
        )
        .await
        .expect("timed out waiting for task status")
    }
}
