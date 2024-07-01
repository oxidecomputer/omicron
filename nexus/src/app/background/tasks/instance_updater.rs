// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for detecting instances in need of update sagas.

use crate::app::background::BackgroundTask;
use crate::app::sagas::instance_update;
use crate::app::sagas::SagaRequest;
use anyhow::Context;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_model::Instance;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::lookup::LookupPath;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::{authn, authz};
use nexus_types::identity::Resource;
use omicron_common::api::external::ListResultVec;
use serde_json::json;
use std::future::Future;
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
        async fn find_instances(
            what: &'static str,
            log: &slog::Logger,
            last_err: &mut Result<(), anyhow::Error>,
            query: impl Future<Output = ListResultVec<Instance>>,
        ) -> Vec<Instance> {
            slog::debug!(&log, "looking for instances with {what}...");
            match query.await {
                Ok(list) => {
                    slog::info!(
                        &log,
                        "listed instances with {what}";
                        "count" => list.len(),
                    );
                    list
                }
                Err(error) => {
                    slog::error!(
                        &log,
                        "failed to list instances with {what}";
                        "error" => %error,
                    );
                    *last_err = Err(error).with_context(|| {
                        format!("failed to find instances with {what}",)
                    });
                    Vec::new()
                }
            }
        }

        let mut last_err = Ok(());

        // NOTE(eliza): These don't, strictly speaking, need to be two separate
        // queries, they probably could instead be `OR`ed together in SQL. I
        // just thought it was nice to be able to record the number of instances
        // found separately for each state.
        let destroyed_active_vmms = find_instances(
            "destroyed active VMMs",
            &opctx.log,
            &mut last_err,
            self.datastore.find_instances_with_destroyed_active_vmms(opctx),
        )
        .await;
        stats.destroyed_active_vmms = destroyed_active_vmms.len();

        let terminated_active_migrations = find_instances(
            "terminated active migrations",
            &opctx.log,
            &mut last_err,
            self.datastore
                .find_instances_with_terminated_active_migrations(opctx),
        )
        .await;
        stats.terminated_active_migrations = terminated_active_migrations.len();

        for instance in destroyed_active_vmms
            .iter()
            .chain(terminated_active_migrations.iter())
        {
            let serialized_authn = authn::saga::Serialized::for_opctx(opctx);
            let (.., authz_instance) = LookupPath::new(&opctx, &self.datastore)
                .instance_id(instance.id())
                .lookup_for(authz::Action::Modify)
                .await?;
            let saga = SagaRequest::InstanceUpdate {
                params: instance_update::Params {
                    serialized_authn,
                    authz_instance,
                },
            };
            self.saga_req
                .send(saga)
                .await
                .context("SagaRequest receiver missing")?;
            stats.update_sagas_queued += 1;
        }

        last_err
    }
}

#[derive(Default)]
struct ActivationStats {
    destroyed_active_vmms: usize,
    terminated_active_migrations: usize,
    update_sagas_queued: usize,
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
                        "terminated_active_migrations" => stats.terminated_active_migrations,
                        "update_sagas_queued" => stats.update_sagas_queued,
                    );
                    None
                }
                Err(error) => {
                    slog::warn!(
                        &opctx.log,
                        "instance updater activation failed!";
                        "error" => %error,
                        "destroyed_active_vmms" => stats.destroyed_active_vmms,
                        "terminated_active_migrations" => stats.terminated_active_migrations,
                        "update_sagas_queued" => stats.update_sagas_queued,
                    );
                    Some(error.to_string())
                }
            };
            json!({
                "destroyed_active_vmms": stats.destroyed_active_vmms,
                "terminated_active_migrations": stats.terminated_active_migrations,
                "update_sagas_queued": stats.update_sagas_queued,
                "error": error,
            })
        }
        .boxed()
    }
}
