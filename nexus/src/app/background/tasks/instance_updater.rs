// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for detecting instances in need of update sagas.

use crate::app::background::BackgroundTask;
use crate::app::saga::StartSaga;
use crate::app::sagas::instance_update;
use crate::app::sagas::NexusSaga;
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
use tokio::task::JoinSet;

pub struct InstanceUpdater {
    datastore: Arc<DataStore>,
    sagas: Arc<dyn StartSaga>,
}

impl InstanceUpdater {
    pub fn new(datastore: Arc<DataStore>, sagas: Arc<dyn StartSaga>) -> Self {
        InstanceUpdater { datastore, sagas }
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
        let mut sagas = JoinSet::new();

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
        self.start_sagas(
            &opctx,
            stats,
            &mut last_err,
            &mut sagas,
            destroyed_active_vmms,
        )
        .await;

        let terminated_active_migrations = find_instances(
            "terminated active migrations",
            &opctx.log,
            &mut last_err,
            self.datastore
                .find_instances_with_terminated_active_migrations(opctx),
        )
        .await;
        stats.terminated_active_migrations = terminated_active_migrations.len();
        self.start_sagas(
            &opctx,
            stats,
            &mut last_err,
            &mut sagas,
            terminated_active_migrations,
        )
        .await;

        // Now, wait for the sagas to complete.
        while let Some(saga_result) = sagas.join_next().await {
            match saga_result {
                Err(err) => {
                    debug_assert!(
                        false,
                        "since nexus is compiled with `panic=\"abort\"`, and \
                         we never cancel the tasks on the `JoinSet`, a \
                         `JoinError` should never be observed!",
                    );
                    stats.sagas_failed += 1;
                    last_err = Err(err.into());
                }
                Ok(Err(err)) => {
                    warn!(opctx.log, "update saga failed!"; "error" => %err);
                    stats.sagas_failed += 1;
                    last_err = Err(err.into());
                }
                Ok(Ok(())) => stats.sagas_completed += 1,
            }
        }

        last_err
    }

    async fn start_sagas(
        &self,
        opctx: &OpContext,
        stats: &mut ActivationStats,
        last_err: &mut Result<(), anyhow::Error>,
        sagas: &mut JoinSet<Result<(), anyhow::Error>>,
        instances: impl IntoIterator<Item = Instance>,
    ) {
        let serialized_authn = authn::saga::Serialized::for_opctx(opctx);
        for instance in instances {
            let instance_id = instance.id();
            let saga = async {
                let (.., authz_instance) =
                    LookupPath::new(&opctx, &self.datastore)
                        .instance_id(instance_id)
                        .lookup_for(authz::Action::Modify)
                        .await?;
                instance_update::SagaInstanceUpdate::prepare(
                    &instance_update::Params {
                        serialized_authn: serialized_authn.clone(),
                        authz_instance,
                    },
                )
                .with_context(|| {
                    format!("failed to prepare instance-update saga for {instance_id}")
                })
            }
            .await;
            match saga {
                Ok(saga) => {
                    let start_saga = self.sagas.clone();
                    sagas.spawn(async move {
                        start_saga.saga_start(saga).await.with_context(|| {
                            format!("update saga for {instance_id} failed")
                        })
                    });
                    stats.sagas_started += 1;
                }
                Err(err) => {
                    warn!(
                        opctx.log,
                        "failed to start instance-update saga!";
                        "instance_id" => %instance_id,
                        "error" => %err,
                    );
                    stats.saga_start_failures += 1;
                    *last_err = Err(err);
                }
            }
        }
    }
}

#[derive(Default)]
struct ActivationStats {
    destroyed_active_vmms: usize,
    terminated_active_migrations: usize,
    sagas_started: usize,
    sagas_completed: usize,
    sagas_failed: usize,
    saga_start_failures: usize,
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
                        "update_sagas_started" => stats.sagas_started,
                        "update_sagas_completed" => stats.sagas_completed,
                    );
                    debug_assert_eq!(
                        stats.sagas_failed,
                        0,
                        "if the task completed successfully, then no sagas \
                         should have failed",
                    );
                    debug_assert_eq!(
                        stats.saga_start_failures,
                        0,
                        "if the task completed successfully, all sagas \
                         should have started successfully"
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
                        "update_sagas_started" => stats.sagas_started,
                        "update_sagas_completed" => stats.sagas_completed,
                        "update_sagas_failed" => stats.sagas_failed,
                        "update_saga_start_failures" => stats.saga_start_failures,
                    );
                    Some(error.to_string())
                }
            };
            json!({
                "destroyed_active_vmms": stats.destroyed_active_vmms,
                "terminated_active_migrations": stats.terminated_active_migrations,
                "sagas_started": stats.sagas_started,
                "sagas_completed": stats.sagas_completed,
                "sagas_failed": stats.sagas_failed,
                "saga_start_failures": stats.saga_start_failures,
                "error": error,
            })
        }
        .boxed()
    }
}
