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
        let (tx, _rx) = watch::channel(
            ReconfiguratorConfigLoaderState::NotYetLoaded,
        );
        Self { datastore, tx }
    }

    pub fn watcher(
        &self,
    ) -> watch::Receiver<ReconfiguratorConfigLoaderState> {
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
                    let switches =
                        ReconfiguratorConfigLoaderState::Loaded(
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
    use async_bb8_diesel::AsyncRunQueryDsl;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::deployment::{
        PlannerConfig, ReconfiguratorConfig, ReconfiguratorConfigParam,
    };

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
            use nexus_db_schema::schema::reconfigurator_chicken_switches::dsl;
            let conn = datastore.pool_connection_for_tests().await.unwrap();
            diesel::delete(dsl::reconfigurator_chicken_switches)
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
            ReconfiguratorConfigLoaderState::Loaded(
                default_switches.clone()
            )
        );

        // Insert an initial set of switches.
        let expected_switches = ReconfiguratorConfig {
            planner_enabled: !default_switches.config.planner_enabled,
            planner_config: PlannerConfig::default(),
        };
        let switches =
            ReconfiguratorConfigParam { version: 1, config: expected_switches };
        datastore
            .reconfigurator_config_insert_latest_version(
                &opctx, switches,
            )
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
        };
        let switches =
            ReconfiguratorConfigParam { version: 2, config: expected_switches };
        datastore
            .reconfigurator_config_insert_latest_version(
                &opctx, switches,
            )
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
}
