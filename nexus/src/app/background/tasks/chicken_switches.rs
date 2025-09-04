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
use nexus_types::deployment::ReconfiguratorChickenSwitchesView;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::watch;

/// Enum that allows downstream tasks to wait until this task has had a chance
/// to read the current chicken switches from the database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconfiguratorChickenSwitchesLoaderState {
    NotYetLoaded,
    Loaded(ReconfiguratorChickenSwitchesView),
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
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::deployment::{
        PlannerChickenSwitches, ReconfiguratorChickenSwitches,
        ReconfiguratorChickenSwitchesParam,
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

        let mut task = ChickenSwitchesLoader::new(datastore.clone());
        let out = task.activate(&opctx).await;
        assert_eq!(out["chicken_switches_updated"], false);
        let switches = ReconfiguratorChickenSwitchesParam {
            version: 1,
            switches: ReconfiguratorChickenSwitches {
                planner_enabled: true,
                planner_switches: PlannerChickenSwitches::default(),
            },
        };
        datastore
            .reconfigurator_chicken_switches_insert_latest_version(
                &opctx, switches,
            )
            .await
            .unwrap();
        let out = task.activate(&opctx).await;
        assert_eq!(out["chicken_switches_updated"], true);
        let out = task.activate(&opctx).await;
        assert_eq!(out["chicken_switches_updated"], false);
        let switches = ReconfiguratorChickenSwitchesParam {
            version: 2,
            switches: ReconfiguratorChickenSwitches {
                planner_enabled: false,
                planner_switches: PlannerChickenSwitches::default(),
            },
        };
        datastore
            .reconfigurator_chicken_switches_insert_latest_version(
                &opctx, switches,
            )
            .await
            .unwrap();
        let out = task.activate(&opctx).await;
        assert_eq!(out["chicken_switches_updated"], true);
    }
}
