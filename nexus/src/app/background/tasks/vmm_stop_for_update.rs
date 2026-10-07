// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task that stops VMMs that have been marked as needing to be
//! stopped for a sled update.

use crate::app::background::BackgroundTask;
use crate::app::instance::SledAgentInstanceError;
use futures::future::BoxFuture;
use iddqd::IdOrdMap;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore::SQL_BATCH_SIZE;
use nexus_db_queries::db::pagination::Paginator;
use nexus_types::internal_api::background::VmmStopForUpdateStatus;
use nexus_types::internal_api::background::VmmsBySled;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::PropolisUuid;
use serde_json::json;
use sled_agent_client::types::VmmPutStateBody;
use sled_agent_client::types::VmmStateRequested;
use sled_agent_types::instance::SledVmmState;
use sled_agent_types::instance::VmmRuntimeState;
use sled_agent_types::instance::VmmState;
use slog_error_chain::InlineErrorChain;
use std::sync::Arc;

struct VmmStopForUpdateResult {
    vmms_stopped_by_sled: IdOrdMap<VmmsBySled>,
    vmms_failed_by_sled: IdOrdMap<VmmsBySled>,
    errors: Vec<Error>,
}

impl VmmStopForUpdateResult {
    fn new() -> Self {
        Self {
            vmms_stopped_by_sled: IdOrdMap::new(),
            vmms_failed_by_sled: IdOrdMap::new(),
            errors: Vec::new(),
        }
    }

    /// Merges the values of `self` with a given [`VmmStopForUpdateResult`]
    fn merge(&mut self, other: VmmStopForUpdateResult) {
        let VmmStopForUpdateResult {
            vmms_stopped_by_sled,
            vmms_failed_by_sled,
            errors,
        } = other;

        for VmmsBySled { sled_id, vmm_ids } in vmms_stopped_by_sled {
            self.vmms_stopped_by_sled
                .entry(sled_id)
                .or_insert_with(|| VmmsBySled { sled_id, vmm_ids: Vec::new() })
                .vmm_ids
                .extend(vmm_ids);
        }
        for VmmsBySled { sled_id, vmm_ids } in vmms_failed_by_sled {
            self.vmms_failed_by_sled
                .entry(sled_id)
                .or_insert_with(|| VmmsBySled { sled_id, vmm_ids: Vec::new() })
                .vmm_ids
                .extend(vmm_ids);
        }
        self.errors.extend(errors);
    }
}

pub struct VmmStopForUpdate {
    datastore: Arc<DataStore>,
}

impl VmmStopForUpdate {
    pub fn new(datastore: Arc<DataStore>) -> Self {
        Self { datastore }
    }

    /// Retrieves a list of VMMs marked to be stopped for update by sled, and
    /// stops them in batches.
    async fn stop_all(&self, opctx: &OpContext) -> VmmStopForUpdateResult {
        let mut result = VmmStopForUpdateResult::new();
        let mut paginator = Paginator::new(
            SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        while let Some(p) = paginator.next() {
            let vmms = match self
                .datastore
                .vmm_list_marked_stop_for_update(opctx, &p.current_pagparams())
                .await
            {
                Ok(vmms) => vmms,
                Err(err) => {
                    result.errors.push(err);
                    break;
                }
            };
            paginator = p.found_batch(&vmms, &|vmm| vmm.id);

            let mut vmms_by_sled = IdOrdMap::new();
            for vmm in vmms {
                let sled_id = vmm.sled_id();
                vmms_by_sled
                    .entry(sled_id)
                    .or_insert_with(|| VmmsBySled {
                        sled_id,
                        vmm_ids: Vec::new(),
                    })
                    .vmm_ids
                    .push(PropolisUuid::from_untyped_uuid(vmm.id));
            }
            let batch_result = self.stop_batch(vmms_by_sled, opctx).await;
            result.merge(batch_result);
        }

        result
    }

    /// Stops a batch of VMMs
    async fn stop_batch(
        &self,
        vmms_by_sled: IdOrdMap<VmmsBySled>,
        opctx: &OpContext,
    ) -> VmmStopForUpdateResult {
        let mut result = VmmStopForUpdateResult::new();

        for VmmsBySled { sled_id, vmm_ids } in &vmms_by_sled {
            slog::info!(
                opctx.log,
                "Stopping VMMs for update";
                "sled_id" => %sled_id,
                "vmms" => ?vmm_ids,
            );

            // Since we're using IdOrdMap, each sled id will only appear once.
            // Because of this it's not a big deal to start a new sled agent
            // client per sled id.
            let sled_client = match nexus_networking::sled_client(
                &self.datastore,
                &opctx,
                *sled_id,
                &opctx.log,
            )
            .await
            {
                Ok(client) => client,
                Err(e) => {
                    slog::error!(
                        opctx.log,
                        "Failed to create sled agent client";
                        "sled_id" => %sled_id,
                        InlineErrorChain::new(&e),
                    );

                    result.errors.push(e.clone());

                    continue;
                }
            };

            for id in vmm_ids {
                let response = match sled_client
                    .vmm_put_state(
                        id,
                        &VmmPutStateBody { state: VmmStateRequested::Stopped },
                    )
                    .await
                {
                    Ok(res) => res.into_inner().updated_runtime,
                    Err(e) => {
                        let err = SledAgentInstanceError(e);

                        slog::error!(
                            opctx.log,
                            "Failed to mark VMM as Stopped";
                            "sled_id" => %sled_id,
                            "vmm_id" => %id,
                            InlineErrorChain::new(&err),
                        );

                        result.errors.push(err.into());
                        result
                            .vmms_failed_by_sled
                            .entry(*sled_id)
                            .or_insert_with(|| VmmsBySled {
                                sled_id: *sled_id,
                                vmm_ids: Vec::new(),
                            })
                            .vmm_ids
                            .push(*id);

                        continue;
                    }
                };

                match response {
                    None => {
                        slog::debug!(
                            opctx.log,
                            "VMM already stopped, no changes to state";
                            "sled_id" => %sled_id,
                            "vmm_id" => %id
                        );
                    }
                    Some(vmm) => {
                        let SledVmmState {
                            vmm_state,
                            migration_in: _,
                            migration_out: _,
                        } = vmm;

                        let VmmRuntimeState { state, generation, time_updated } =
                            vmm_state;

                        match state {
                            // If we succeeded, one of three things happened:
                            //
                            //  - The VMM has not stopped yet, but propolis has
                            //    accepted the `put_state` request and reports
                            //    the VMM's state as `Stopping`.
                            //  - A propolis zone does not exist yet. In this
                            //    scenario a VMM could have been in the
                            //    `Creating` state but had not reached the point
                            //    where the propolis zone was created. In this
                            //    case the request succeeded, but the VMM is
                            //    reported as `Destroyed`.
                            //  - A VMM should technically never be reported as
                            //    `Stopped` since
                            //    propolis_client::types::InstanceState::Stopped
                            //    is always reported as `VmmState::Stopping`.
                            //    But, if for some reason we get this state,
                            //    it's what we want, so we consider it a
                            //    success.
                            VmmState::Stopped
                            | VmmState::Stopping
                            | VmmState::Destroyed => {
                                result
                                    .vmms_stopped_by_sled
                                    .entry(*sled_id)
                                    .or_insert_with(|| VmmsBySled {
                                        sled_id: *sled_id,
                                        vmm_ids: Vec::new(),
                                    })
                                    .vmm_ids
                                    .push(*id);

                                slog::debug!(
                                    opctx.log,
                                    "Stopped VMM for sled evacuation during \
                                    an update";
                                    "sled_id" => %sled_id,
                                    "vmm_id" => %id,
                                    "vmm_generation" => ?generation,
                                    "time_updated" => ?time_updated,
                                );
                            }
                            // Propolis is alive but does not find a VMM. This
                            // implies that it restarted and lost the previously
                            // created VMM. The VMM is reported as Failed. It's
                            // not necessary to stop it anymore, so we don't add
                            // it to the `vmms_failed_by_sled` list.
                            VmmState::Failed => {
                                slog::info!(
                                    opctx.log,
                                    "Unable to find VMM, possibly due to a \
                                    restart. No need to stop it";
                                    "sled_id" => %sled_id,
                                    "vmm_id" => %id,
                                    "state" => ?state,
                                    "vmm_generation" => ?generation,
                                    "time_updated" => ?time_updated,
                                );
                            }
                            VmmState::Migrating
                            | VmmState::Rebooting
                            | VmmState::Starting
                            | VmmState::Running => {
                                slog::error!(
                                    opctx.log,
                                    "Failed to mark VMM as Stopped, \
                                    unexpected state reported";
                                    "sled_id" => %sled_id,
                                    "vmm_id" => %id,
                                    "state" => ?state,
                                    "vmm_generation" => ?generation,
                                    "time_updated" => ?time_updated,
                                );
                            }
                        }
                    }
                }
            }
        }

        result
    }

    pub(crate) async fn actually_activate(
        &mut self,
        opctx: &OpContext,
    ) -> VmmStopForUpdateStatus {
        let VmmStopForUpdateResult {
            vmms_stopped_by_sled,
            vmms_failed_by_sled,
            errors,
        } = self.stop_all(opctx).await;

        let error_messages: Vec<String> = errors
            .iter()
            .map(|err| InlineErrorChain::new(err).to_string())
            .collect();

        if vmms_stopped_by_sled.is_empty() {
            slog::info!(
                &opctx.log,
                "no VMMs were stopped for a sled update";
            );
        } else {
            for sled in &vmms_stopped_by_sled {
                let VmmsBySled { sled_id, vmm_ids } = sled;
                slog::info!(
                    &opctx.log,
                    "stopped {} VMMs marked to stop for a sled update",
                    vmm_ids.len();
                    "sled_id" => %sled_id,
                    "vmms" => ?vmm_ids,
                );
            }
        }

        if !vmms_failed_by_sled.is_empty() {
            for sled in &vmms_failed_by_sled {
                let VmmsBySled { sled_id, vmm_ids } = sled;
                slog::info!(
                    &opctx.log,
                    "failed to stop {} VMMs marked to stop for a sled update",
                    vmm_ids.len();
                    "sled_id" => %sled_id,
                    "vmms" => ?vmm_ids,
                );
            }
        }

        if !error_messages.is_empty() {
            slog::error!(
                &opctx.log,
                "failed to stop VMMs marked to stop for a sled update";
                "errors" => ?error_messages,
            );
        }

        VmmStopForUpdateStatus {
            vmms_stopped_by_sled,
            vmms_failed_by_sled,
            error_messages,
        }
    }
}

impl BackgroundTask for VmmStopForUpdate {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        Box::pin(async {
            let status = self.actually_activate(opctx).await;
            match serde_json::to_value(status) {
                Ok(val) => val,
                Err(err) => {
                    json!({
                        "error": format!("failed to serialize status: {err}")
                    })
                }
            }
        })
    }
}
