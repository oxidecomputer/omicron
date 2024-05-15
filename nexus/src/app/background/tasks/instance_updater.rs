// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for detecting instances in need of update sagas.
//!
//! TODO this is currently a placeholder for a future PR

use super::common::BackgroundTask;
use crate::app::sagas::SagaRequest;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

pub struct InstanceUpdater {
    datastore: Arc<DataStore>,
    saga_req: Sender<SagaRequest>,
}

impl InstanceUpdater {
    pub fn new(
        datastore: Arc<DataStore>,
        saga_req: Sender<SagaRequest>,
    ) -> Self {
        InstanceUpdater { datastore, saga_req }
    }

    async fn activate2(
        &mut self,
        opctx: &OpContext,
    ) -> Result<Updated, anyhow::Error> {
        let mut updated = Updated::default();

        let log = &opctx.log;

        slog::debug!(
            &log,
            "looking for instances with destroyed active VMMs..."
        );

        let destroyed_active_vmms = self
            .datastore
            .find_instances_with_destroyed_active_vmms(opctx)
            .await
            .context("failed to find instances with destroyed active VMMs")?;

        slog::info!(
            &log,
            "listed instances with destroyed active VMMs";
            "count" => destroyed_active_vmms.len(),
        );

        updated.destroyed_active_vmms = destroyed_active_vmms.len();

        for (instance, vmm) in destroyed_active_vmms {
            let saga = SagaRequest::InstanceUpdate {};
        }

        Ok(updated)
    }
}

#[derive(Default)]
struct Updated {
    destroyed_active_vmms: usize,
    sagas_started: usize,
}

impl BackgroundTask for InstanceUpdater {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            match self.activate2(opctx).await {
                Ok(updated) => json!({
                    "destroyed_active_vmms": updated.destroyed_active_vmms,
                    "error": None,
                }),
                Err(error) => {
                    slog::error!(
                        opctx.log,
                        "failed to start instance update saga(s)";
                        "error" => ?error,
                    );
                    json!({
                        "destroyed_active_vmms": 0,
                        "error": error.to_string(),
                    })
                }
            }
        }
        .boxed()
    }
}
