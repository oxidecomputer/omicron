// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task that stops VMMs that have been marked as needing to be
//! stopped for a sled update.

use crate::app::background::BackgroundTask;
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
                    // TODO-K: Verify how the paginator actually works. Should
                    // I break here or not?
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
        for VmmsBySled { sled_id, vmm_ids } in &vmms_by_sled {
            slog::info!(
                opctx.log,
                "Stopping VMMs for update";
                "sled_id" => %sled_id,
                "vmms" => ?vmm_ids,
            );
            // TODO-K: actually stop the VMMs in `vmms_by_sled` and record
            // how many were stopped on each sled, how many failed, whatever
        }

        // TODO-K: actually return the results of the stopped vmms
        VmmStopForUpdateResult::new()
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

        // TODO-K: Once we have useful information log it
        //    if results.vmms_by_sled.is_empty() {
        //        slog::debug!(
        //            &opctx.log,
        //            "no VMMs were stopped for a sled update";
        //        );
        //    } else {
        //        let vmm_count: usize =
        //            vmms_by_sled.iter().map(|sled| sled.vmm_ids.len()).sum();
        //        slog::info!(
        //            &opctx.log,
        //            "stopped VMMs marked to stop for a sled update";
        //            "sleds" => vmms_by_sled.len(),
        //            "vmms" => vmm_count,
        //        );
        //      TODO-K: for debug show IDs? also show which failed
        //    }

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
                    json!({ "error": format!("failed to serialize status: {err}") })
                }
            }
        })
    }
}
