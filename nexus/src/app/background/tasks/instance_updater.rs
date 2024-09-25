// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for detecting instances in need of update sagas.

use crate::app::background::BackgroundTask;
use crate::app::saga::StartSaga;
use crate::app::sagas::instance_update;
use crate::app::sagas::NexusSaga;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_model::Instance;
use nexus_db_model::VmmState;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::lookup::LookupPath;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::{authn, authz};
use nexus_types::identity::Resource;
use nexus_types::internal_api::background::InstanceUpdaterStatus;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use serde_json::json;
use std::future::Future;
use std::sync::Arc;
use steno::SagaId;
use tokio::task::JoinSet;
use uuid::Uuid;

pub struct InstanceUpdater {
    datastore: Arc<DataStore>,
    sagas: Arc<dyn StartSaga>,
    disable: bool,
}

impl InstanceUpdater {
    pub fn new(
        datastore: Arc<DataStore>,
        sagas: Arc<dyn StartSaga>,
        disable: bool,
    ) -> Self {
        InstanceUpdater { datastore, sagas, disable }
    }

    async fn actually_activate(
        &mut self,
        opctx: &OpContext,
        status: &mut InstanceUpdaterStatus,
    ) {
        async fn find_instances(
            what: &'static str,
            log: &slog::Logger,
            status: &mut InstanceUpdaterStatus,
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
                    const ERR_MSG: &'static str =
                        "failed to list instances with ";
                    slog::error!(
                        &log,
                        "{ERR_MSG} {what}";
                        "error" => &error,
                    );
                    status
                        .query_errors
                        .push(format!("{ERR_MSG} {what}: {error}"));
                    Vec::new()
                }
            }
        }

        let mut sagas = JoinSet::new();

        // NOTE(eliza): These don't, strictly speaking, need to be two separate
        // queries, they probably could instead be `OR`ed together in SQL. I
        // just thought it was nice to be able to record the number of instances
        // found separately for each state.
        let destroyed_active_vmms = find_instances(
            "destroyed active VMMs",
            &opctx.log,
            status,
            self.datastore
                .find_instances_by_active_vmm_state(opctx, VmmState::Destroyed),
        )
        .await;
        status.destroyed_active_vmms = destroyed_active_vmms.len();
        self.start_sagas(&opctx, status, &mut sagas, destroyed_active_vmms)
            .await;

        let failed_active_vmms = find_instances(
            "failed active VMMs",
            &opctx.log,
            status,
            self.datastore
                .find_instances_by_active_vmm_state(opctx, VmmState::Failed),
        )
        .await;
        status.failed_active_vmms = failed_active_vmms.len();
        self.start_sagas(&opctx, status, &mut sagas, failed_active_vmms).await;

        let terminated_active_migrations = find_instances(
            "terminated active migrations",
            &opctx.log,
            status,
            self.datastore
                .find_instances_with_terminated_active_migrations(opctx),
        )
        .await;
        status.terminated_active_migrations =
            terminated_active_migrations.len();
        self.start_sagas(
            &opctx,
            status,
            &mut sagas,
            terminated_active_migrations,
        )
        .await;

        // Now, wait for the sagas to complete.
        while let Some(saga_result) = sagas.join_next().await {
            match saga_result {
                Err(err) => {
                    const MSG: &'static str = "since nexus is compiled with \
                     `panic=\"abort\"`, and we never cancel the tasks on the \
                     `JoinSet`, a `JoinError` should never be observed!";
                    debug_assert!(false, "{MSG}");
                    error!(opctx.log, "{MSG}"; "error" => ?err);
                    status
                        .saga_errors
                        .push((None, format!("unexpected JoinError: {err}")));
                }
                Ok(Err((instance_id, saga_id, err))) => {
                    warn!(
                        opctx.log,
                        "update saga failed!";
                        "instance_id" => %instance_id,
                        "saga_id" => %saga_id,
                        "error" => &err,
                    );
                    status.saga_errors.push((
                        Some(instance_id),
                        format!("update saga {saga_id} failed: {err}"),
                    ));
                }
                Ok(Ok(())) => status.sagas_completed += 1,
            }
        }
    }

    async fn start_sagas(
        &self,
        opctx: &OpContext,
        status: &mut InstanceUpdaterStatus,
        sagas: &mut JoinSet<Result<(), (Uuid, SagaId, Error)>>,
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
                let dag = instance_update::SagaInstanceUpdate::prepare(
                    &instance_update::Params {
                        serialized_authn: serialized_authn.clone(),
                        authz_instance,
                    },
                )?;
                self.sagas.saga_run(dag).await
            }
            .await;
            match saga {
                Ok((saga_id, completed)) => {
                    status.sagas_started += 1;
                    sagas.spawn(async move {
                        completed.await.map_err(|e| (instance_id, saga_id, e))
                    });
                }
                Err(err) => {
                    const ERR_MSG: &str = "failed to start update saga";
                    warn!(
                        opctx.log,
                        "{ERR_MSG}!";
                        "instance_id" => %instance_id,
                        "error" => %err,
                    );
                    status
                        .saga_errors
                        .push((Some(instance_id), format!("{ERR_MSG}: {err}")));
                }
            }
        }
    }
}

impl BackgroundTask for InstanceUpdater {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            let mut status = InstanceUpdaterStatus::default();

            if self.disable {
                slog::info!(&opctx.log, "background instance updater explicitly disabled");
                status.disabled = true;
                return json!(status);
            }

            self.actually_activate(opctx, &mut status).await;
            if status.errors() == 0 {
                slog::info!(
                    &opctx.log,
                    "instance updater activation completed";
                    "destroyed_active_vmms" => status.destroyed_active_vmms,
                    "failed_active_vmms" => status.failed_active_vmms,
                    "terminated_active_migrations" => status.terminated_active_migrations,
                    "update_sagas_started" => status.sagas_started,
                    "update_sagas_completed" => status.sagas_completed,
                );
            } else {
                slog::error!(
                    &opctx.log,
                    "instance updater activation failed!";
                    "query_errors" => status.query_errors.len(),
                    "saga_errors" => status.saga_errors.len(),
                    "destroyed_active_vmms" => status.destroyed_active_vmms,
                    "failed_active_vmms" => status.failed_active_vmms,
                    "terminated_active_migrations" => status.terminated_active_migrations,
                    "update_sagas_started" => status.sagas_started,
                    "update_sagas_completed" => status.sagas_completed,
                );
            };

            json!(status)
        }
        .boxed()
    }
}
