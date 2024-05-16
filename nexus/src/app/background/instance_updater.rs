// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for detecting instances in need of update sagas.
//!
//! TODO this is currently a placeholder for a future PR

use super::common::BackgroundTask;
use crate::app::authn;
use crate::app::sagas::{self, SagaRequest};
use anyhow::Context;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::datastore::{InstanceAndActiveVmm, InstanceAndVmms};
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
        stats: &mut ActivationStats,
    ) -> Result<(), anyhow::Error> {
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

        stats.destroyed_active_vmms = destroyed_active_vmms.len();

        for InstanceAndActiveVmm { instance, vmm } in destroyed_active_vmms {
            let saga = SagaRequest::InstanceUpdate {
                params: sagas::instance_update::Params {
                    serialized_authn: authn::saga::Serialized::for_opctx(opctx),
                    state: InstanceAndVmms {
                        instance,
                        active_vmm: vmm,
                        target_vmm: None,
                    },
                },
            };
            self.saga_req
                .send(saga)
                .await
                .context("SagaRequest receiver missing")?;
            stats.sagas_started += 1;
        }

        Ok(())
    }
}

#[derive(Default)]
struct ActivationStats {
    destroyed_active_vmms: usize,
    sagas_started: usize,
}

impl BackgroundTask for InstanceUpdater {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            let mut stats = ActivationStats::default();
            let error = match self.activate2(opctx, &mut stats).await {
                Ok(()) => {
                    slog::info!(
                        &opctx.log,
                        "instance updater activation completed";
                        "destroyed_active_vmms" => stats.destroyed_active_vmms,
                        "sagas_started" => stats.sagas_started,
                    );
                    None
                }
                Err(error) => {
                    slog::warn!(
                        &opctx.log,
                        "instance updater activation failed!";
                        "error" => %error,
                        "destroyed_active_vmms" => stats.destroyed_active_vmms,
                        "sagas_started" => stats.sagas_started,
                    );
                    Some(error.to_string())
                }
            };
            json!({
                "destroyed_active_vmms": stats.destroyed_active_vmms,
                "sagas_started": stats.sagas_started,
                "error": error,
            })
        }
        .boxed()
    }
}
