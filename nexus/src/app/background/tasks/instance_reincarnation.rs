// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for automatically restarting failed instances.

use crate::app::background::BackgroundTask;
use crate::app::saga::StartSaga;
use crate::app::sagas::instance_start;
use crate::app::sagas::NexusSaga;
use futures::future::BoxFuture;
use nexus_db_queries::authn;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::pagination::Paginator;
use nexus_db_queries::db::DataStore;
use nexus_types::identity::Resource;
use nexus_types::internal_api::background::InstanceReincarnationStatus;
use omicron_common::api::external::Error;
use std::num::NonZeroU32;
use std::sync::Arc;
use tokio::task::JoinSet;

pub struct InstanceReincarnation {
    datastore: Arc<DataStore>,
    sagas: Arc<dyn StartSaga>,
}

const BATCH_SIZE: NonZeroU32 = unsafe {
    // Safety: last time I checked, 100 was greater than zero.
    NonZeroU32::new_unchecked(100)
};

impl BackgroundTask for InstanceReincarnation {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        Box::pin(async move {
            let mut status = InstanceReincarnationStatus::default();
            self.actually_activate(opctx, &mut status).await;
            if !status.restart_errors.is_empty() || status.query_error.is_some()
            {
                error!(
                    &opctx.log,
                    "instance reincarnation completed with errors!";
                    "instances_found" => status.instances_found,
                    "instances_reincarnated" => status.instances_reincarnated.len(),
                    "already_reincarnated" => status.already_reincarnated.len(),
                    "query_error" => ?status.query_error,
                    "restart_errors" => status.restart_errors.len(),
                );
            } else if !status.instances_reincarnated.is_empty() {
                info!(
                    &opctx.log,
                    "instance reincarnation completed";
                    "instances_found" => status.instances_found,
                    "instances_reincarnated" => status.instances_reincarnated.len(),
                    "already_reincarnated" => status.already_reincarnated.len(),
                );
            } else {
                debug!(
                    &opctx.log,
                    "instance reincarnation completed; no instances \
                     in need of reincarnation";
                    "instances_found" => status.instances_found,
                    "already_reincarnated" => status.already_reincarnated.len(),
                );
            };
            serde_json::json!(status)
        })
    }
}

impl InstanceReincarnation {
    pub(crate) fn new(
        datastore: Arc<DataStore>,
        sagas: Arc<dyn StartSaga>,
    ) -> Self {
        Self { datastore, sagas }
    }

    async fn actually_activate(
        &mut self,
        opctx: &OpContext,
        status: &mut InstanceReincarnationStatus,
    ) {
        let mut tasks = JoinSet::new();
        let mut paginator = Paginator::new(BATCH_SIZE);

        while let Some(p) = paginator.next() {
            let maybe_batch = self
                .datastore
                .find_reincarnatable_instances(opctx, &p.current_pagparams())
                .await;
            let batch = match maybe_batch {
                Ok(batch) => batch,
                Err(error) => {
                    error!(
                        opctx.log,
                        "failed to list instances in need of reincarnation";
                        "error" => &error,
                    );
                    status.query_error = Some(error.to_string());
                    break;
                }
            };

            paginator = p.found_batch(&batch, &|instance| instance.id());

            let found = batch.len();
            if found == 0 {
                debug!(
                    opctx.log,
                    "no more instances in need of reincarnation";
                    "total_found" => status.instances_found,
                );
                break;
            }

            let prev_sagas_started = tasks.len();
            status.instances_found += found;

            let serialized_authn = authn::saga::Serialized::for_opctx(opctx);
            for db_instance in batch {
                let instance_id = db_instance.id();
                let prepared_saga = instance_start::SagaInstanceStart::prepare(
                    &instance_start::Params {
                        db_instance,
                        serialized_authn: serialized_authn.clone(),
                    },
                );
                match prepared_saga {
                    Ok(saga) => {
                        let start_saga = self.sagas.clone();
                        tasks.spawn(async move {
                            start_saga
                                .saga_start(saga)
                                .await
                                .map_err(|e| (instance_id, e))?;
                            Ok(instance_id)
                        });
                    }
                    Err(error) => {
                        const ERR_MSG: &'static str =
                            "failed to prepare instance-start saga";
                        error!(
                            opctx.log,
                            "{ERR_MSG} for {instance_id}";
                            "instance_id" => %instance_id,
                            "error" => %error,
                        );
                        status
                            .restart_errors
                            .push((instance_id, format!("{ERR_MSG}: {error}")))
                    }
                };
            }

            let total_sagas_started = tasks.len();
            debug!(
                opctx.log,
                "found instance in need of reincarnation";
                "instances_found" => found,
                "total_found" => status.instances_found,
                "sagas_started" => total_sagas_started - prev_sagas_started,
                "total_sagas_started" => total_sagas_started,
            );
        }

        // All sagas started, wait for them to come back...
        while let Some(saga_result) = tasks.join_next().await {
            match saga_result {
                // Start saga completed successfully
                Ok(Ok(instance_id)) => {
                    debug!(
                        opctx.log,
                        "welcome back to the realm of the living, {instance_id}!";
                        "instance_id" => %instance_id,
                    );
                    status.instances_reincarnated.push(instance_id);
                }
                // The instance was restarted by another saga, that's fine...
                Ok(Err((instance_id, Error::Conflict { message })))
                    if message.external_message()
                        == instance_start::ALREADY_STARTING_ERROR =>
                {
                    debug!(
                        opctx.log,
                        "instance {instance_id} was already reincarnated";
                        "instance_id" => %instance_id,
                    );
                    status.already_reincarnated.push(instance_id);
                }
                // Start saga failed
                Ok(Err((instance_id, error))) => {
                    const ERR_MSG: &'static str = "instance-start saga failed";
                    warn!(opctx.log,
                        "{ERR_MSG}";
                        "instance_id" => %instance_id,
                        "error" => %error,
                    );
                    status
                        .restart_errors
                        .push((instance_id, format!("{ERR_MSG}: {error}")));
                }
                Err(e) => {
                    const JOIN_ERR_MSG: &'static str =
                        "tasks spawned on the JoinSet should never return a \
                        JoinError, as nexus is compiled with panic=\"abort\", \
                        and we never cancel them...";
                    error!(opctx.log, "{JOIN_ERR_MSG}"; "error" => %e);
                    if cfg!(debug_assertions) {
                        unreachable!("{JOIN_ERR_MSG} but, I saw {e}!",)
                    }
                }
            }
        }
    }
}
