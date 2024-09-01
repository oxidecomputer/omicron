// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for automatically restarting failed instances.

use crate::app::background::BackgroundTask;
use crate::app::saga::StartSaga;
use crate::app::sagas::instance_start;
use crate::app::sagas::NexusSaga;
use anyhow::Context;
use futures::future::BoxFuture;
use nexus_db_queries::authn;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::pagination::Paginator;
use nexus_db_queries::db::DataStore;
use nexus_types::identity::Resource;
use omicron_common::api::external::Error;
use std::num::NonZeroU32;
use std::sync::Arc;
use tokio::task::JoinSet;

pub struct InstanceReincarnation {
    datastore: Arc<DataStore>,
    sagas: Arc<dyn StartSaga>,
}

#[derive(Default)]
struct ActivationStats {
    instances_found: usize,
    instances_reincarnated: usize,
    already_reincarnated: usize,
    sagas_started: usize,
    saga_start_errors: usize,
    saga_errors: usize,
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
            let mut stats = ActivationStats::default();
            let error = match self.actually_activate(opctx, &mut stats).await {
                Ok(_) => {
                    if stats.instances_reincarnated > 0 {
                        info!(
                            &opctx.log,
                            "instance reincarnation completed";
                            "instances_found" => stats.instances_found,
                            "instances_reincarnated" => stats.instances_reincarnated,
                            "already_reincarnated" => stats.already_reincarnated,
                            "sagas_started" => stats.sagas_started,
                        );
                    } else {
                        debug!(
                            &opctx.log,
                            "instance reincarnation completed; no instances \
                             in need of reincarnation";
                            "instances_found" => stats.instances_found,
                            "already_reincarnated" => stats.already_reincarnated,
                        );
                    }
                    None
                }
                Err(error) => {
                    error!(
                        &opctx.log,
                        "instance reincarnation failed!";
                        "last_error" => %error,
                        "instances_found" => stats.instances_found,
                        "instances_reincarnated" => stats.instances_reincarnated,
                        "already_reincarnated" => stats.already_reincarnated,
                        "sagas_started" => stats.sagas_started,
                        "saga_start_errors" => stats.saga_start_errors,
                        "saga_errors" => stats.saga_errors,
                    );
                    Some(error.to_string())
                }
            };
            serde_json::json!({
                "instances_found": stats.instances_found,
                "instances_reincarnated": stats.instances_reincarnated,
                "already_reincarnated": stats.already_reincarnated,
                "sagas_started": stats.sagas_started,
                "saga_start_errors": stats.saga_start_errors,
                "saga_errors": stats.saga_errors,
                "last_error": error,
            })
        })
    }
}

impl InstanceReincarnation {
    pub(crate) fn new(
        datastore: Arc<Datastore>,
        sagas: Arc<dyn StartSaga>,
    ) -> Self {
        Self { datastore, sagas }
    }

    async fn actually_activate(
        &mut self,
        opctx: &OpContext,
        stats: &mut ActivationStats,
    ) -> anyhow::Result<()> {
        let mut tasks = JoinSet::new();

        let mut last_err = Ok(());
        let mut paginator = Paginator::new(BATCH_SIZE);

        while let Some(p) = paginator.next() {
            let maybe_batch = self
                .datastore
                .find_reincarnatable_instances(opctx, &p.current_pagparams())
                .await;
            let batch = match maybe_batch {
                Ok(batch) => batch,
                Err(error) => {
                    const ERR_STR: &'static str =
                        "failed to list instances in need of reincarnation";
                    error!(
                        opctx.log,
                        "failed to list instances in need of reincarnation";
                        "error" => &error,
                    );
                    last_err = Err(error).context(ERR_STR);
                    break;
                }
            };

            paginator = p.found_batch(&batch, &|instance| instance.id());

            let found = batch.len();
            if found == 0 {
                debug!(
                    opctx.log,
                    "no more instances in need of reincarnation";
                    "total_found" => stats.instances_found,
                );
                break;
            }

            let prev_sagas_started = stats.sagas_started;
            stats.instances_found += found;

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
                        stats.sagas_started += 1;
                    }
                    Err(error) => {
                        const ERR_STR: &'static str =
                            "failed to prepare instance-start saga for ";
                        error!(
                            opctx.log,
                            "{ERR_STR}{instance_id}";
                            "instance_id" => %instance_id,
                            "error" => %error,
                        );
                        last_err = Err(error)
                            .with_context(|| format!("{ERR_STR}{instance_id}"));
                        stats.saga_start_errors += 1;
                    }
                };
            }

            debug!(
                opctx.log,
                "found instance in need of reincarnation";
                "instances_found" => found,
                "total_found" => stats.instances_found,
                "sagas_started" => stats.sagas_started - prev_sagas_started,
                "total_sagas_started" => stats.sagas_started,
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
                    stats.instances_reincarnated += 1;
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
                    stats.already_reincarnated += 1;
                }
                // Start saga failed
                Ok(Err((instance_id, error))) => {
                    const ERR_MSG: &'static str = "failed to restart instance";
                    warn!(opctx.log,
                        "{ERR_MSG} {instance_id}";
                        "instance_id" => %instance_id,
                        "error" => %error,
                    );
                    stats.saga_errors += 1;
                    last_err = Err(error)
                        .with_context(|| format!("{ERR_MSG} {instance_id}"));
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

        last_err
    }
}
