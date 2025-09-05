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
/// to read the current chicken switches from the database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconfiguratorChickenSwitchesLoaderState {
    NotYetLoaded,
    Loaded(ReconfiguratorConfigView),
}

/// Background task that tracks reconfigurator chicken switches from the DB
pub struct ChickenSwitchesLoader {
    datastore: Arc<DataStore>,
    tx: watch::Sender<ReconfiguratorChickenSwitchesLoaderState>,
}

impl ChickenSwitchesLoader {
    pub fn new(datastore: Arc<DataStore>) -> Self {
        let (tx, _rx) = watch::channel(
            ReconfiguratorChickenSwitchesLoaderState::NotYetLoaded,
        );
        Self { datastore, tx }
    }

    pub fn watcher(
        &self,
    ) -> watch::Receiver<ReconfiguratorChickenSwitchesLoaderState> {
        self.tx.subscribe()
    }
}

impl BackgroundTask for ChickenSwitchesLoader {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            match self
                .datastore
                .reconfigurator_chicken_switches_get_latest(opctx)
                .await
                .context("failed to load chicken switches")
            {
                Err(error) => {
                    let message = format!("{:#}", error);
                    warn!(opctx.log, "chicken switches load failed";
                        "error" => message.clone());
                    json!({ "error": message })
                }
                Ok(switches) => {
                    let switches =
                        ReconfiguratorChickenSwitchesLoaderState::Loaded(
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
                        opctx.log, "chicken switches load complete";
                        "switches" => ?switches,
                    );
                    json!({ "chicken_switches_updated": updated })
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

        // `#[nexus_test]` inserts an initial set of chicken switch values to
        // disable planning in general; let's remove that value so we can test
        // from a clean slate.
        //
        // Chicken switch values are supposed to form a continuous history, so
        // there's no datastore method to delete existing values. We'll go
        // behind its back and delete them directly.
        {
            use nexus_db_schema::schema::reconfigurator_chicken_switches::dsl;
            let conn = datastore.pool_connection_for_tests().await.unwrap();
            diesel::delete(dsl::reconfigurator_chicken_switches)
                .execute_async(&*conn)
                .await
                .expect("removed nexus_test default chicken switches");
        }

        let mut task = ChickenSwitchesLoader::new(datastore.clone());

        // Initial state should be `NotYetLoaded`.
        let mut rx = task.watcher();
        assert_eq!(
            *rx.borrow_and_update(),
            ReconfiguratorChickenSwitchesLoaderState::NotYetLoaded
        );

        // We haven't inserted anything into the DB, so the initial activation
        // should populate the channel with our default values.
        let default_switches = ReconfiguratorConfigView::default();
        let out = task.activate(&opctx).await;
        assert_eq!(out["chicken_switches_updated"], true);
        assert!(rx.has_changed().unwrap());
        assert_eq!(
            *rx.borrow_and_update(),
            ReconfiguratorChickenSwitchesLoaderState::Loaded(
                default_switches.clone()
            )
        );

        // Insert an initial set of switches.
        let expected_switches = ReconfiguratorConfig {
            planner_enabled: !default_switches.switches.planner_enabled,
            planner_switches: PlannerConfig::default(),
        };
        let switches = ReconfiguratorConfigParam {
            version: 1,
            switches: expected_switches,
        };
        datastore
            .reconfigurator_chicken_switches_insert_latest_version(
                &opctx, switches,
            )
            .await
            .unwrap();
        let out = task.activate(&opctx).await;
        assert_eq!(out["chicken_switches_updated"], true);
        assert!(rx.has_changed().unwrap());
        {
            let view = match rx.borrow_and_update().clone() {
                ReconfiguratorChickenSwitchesLoaderState::NotYetLoaded => {
                    panic!("unexpected value")
                }
                ReconfiguratorChickenSwitchesLoaderState::Loaded(view) => view,
            };
            assert_eq!(view.version, 1);
            assert_eq!(view.switches, expected_switches);
        }

        // Activating again should not change things.
        let out = task.activate(&opctx).await;
        assert_eq!(out["chicken_switches_updated"], false);
        assert!(!rx.has_changed().unwrap());

        // Insert a new version.
        let expected_switches = ReconfiguratorConfig {
            planner_enabled: !expected_switches.planner_enabled,
            planner_switches: PlannerConfig::default(),
        };
        let switches = ReconfiguratorConfigParam {
            version: 2,
            switches: expected_switches,
        };
        datastore
            .reconfigurator_chicken_switches_insert_latest_version(
                &opctx, switches,
            )
            .await
            .unwrap();
        let out = task.activate(&opctx).await;
        assert_eq!(out["chicken_switches_updated"], true);
        assert!(rx.has_changed().unwrap());
        {
            let view = match rx.borrow_and_update().clone() {
                ReconfiguratorChickenSwitchesLoaderState::NotYetLoaded => {
                    panic!("unexpected value")
                }
                ReconfiguratorChickenSwitchesLoaderState::Loaded(view) => view,
            };
            assert_eq!(view.version, 2);
            assert_eq!(view.switches, expected_switches);
        }
    }
}
