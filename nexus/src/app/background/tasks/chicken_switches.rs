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
use nexus_types::deployment::ReconfiguratorChickenSwitches;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::watch;

/// Background task that tracks reconfigurator chicken switches from the DB
pub struct ChickenSwitchesCollector {
    datastore: Arc<DataStore>,
    tx: watch::Sender<Option<ReconfiguratorChickenSwitches>>,
    rx: watch::Receiver<Option<ReconfiguratorChickenSwitches>>,
}

impl ChickenSwitchesCollector {
    pub fn new(datastore: Arc<DataStore>) -> Self {
        let (tx, rx) = watch::channel(None);
        Self { datastore, tx, rx }
    }

    pub fn watcher(
        &self,
    ) -> watch::Receiver<Option<ReconfiguratorChickenSwitches>> {
        self.rx.clone()
    }
}

impl BackgroundTask for ChickenSwitchesCollector {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            match self
                .datastore
                .reconfigurator_chicken_switches_get_latest(opctx)
                .await
                .context("failed to collect chicken switches")
            {
                Err(error) => {
                    let message = format!("{:#}", error);
                    warn!(opctx.log, "chicken switches collection failed";
                        "error" => message.clone());
                    json!({ "error": message })
                }
                Ok(switches) => {
                    let updated = self.tx.send_if_modified(|s| {
                        if *s != switches {
                            *s = switches;
                            return true;
                        }
                        false
                    });
                    debug!(opctx.log, "chicken switches collection complete");
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
    use nexus_types::deployment::ReconfiguratorChickenSwitchesParam;

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

        let mut task = ChickenSwitchesCollector::new(datastore.clone());
        let out = task.activate(&opctx).await;
        assert_eq!(out["chicken_switches_updated"], false);
        let switches = ReconfiguratorChickenSwitchesParam {
            version: 1,
            planner_enabled: true,
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
            planner_enabled: false,
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
