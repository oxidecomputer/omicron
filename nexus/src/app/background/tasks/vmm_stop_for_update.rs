// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! TODO-K: Fix comment, Background task that stops the VMMs that have been
//! marked as needing to be stopped in order to update their sled.
//!
//! TODO-K: Fix comment, VMMs are marked (via
//! `stop_for_update_disposition_generation` on the `vmm` table) by the
//! `vmm_mark_stop_for_update` background task, which activates this task
//! whenever it marks one or more VMMs. See RFD 739.

use crate::app::background::BackgroundTask;
use futures::future::BoxFuture;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore::SQL_BATCH_SIZE;
use nexus_db_queries::db::pagination::Paginator;
use nexus_types::internal_api::background::VmmStopForUpdateStatus;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::PropolisUuid;
use omicron_uuid_kinds::SledUuid;
use serde_json::json;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::sync::Arc;

/// The VMMs on a single sled that are marked to be stopped in order to update
/// that sled.
#[derive(Clone, Debug)]
struct SledVmmsToStop {
    sled_id: SledUuid,
    vmm_ids: Vec<PropolisUuid>,
}

impl IdOrdItem for SledVmmsToStop {
    type Key<'a> = SledUuid;

    fn key(&self) -> Self::Key<'_> {
        self.sled_id
    }

    id_upcast!();
}

pub struct VmmStopForUpdate {
    datastore: Arc<DataStore>,
}

impl VmmStopForUpdate {
    pub fn new(datastore: Arc<DataStore>) -> Self {
        Self { datastore }
    }

    /// TODO-K: Fix comment, Lists every non-deleted VMM that is marked to be
    /// stopped for a sled update, and groups their IDs by the sled they run on.
    async fn vmms_to_stop_by_sled(
        &self,
        opctx: &OpContext,
    ) -> Result<IdOrdMap<SledVmmsToStop>, Error> {
        let mut vmms_by_sled = IdOrdMap::new();
        let mut paginator = Paginator::new(
            SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        while let Some(p) = paginator.next() {
            let vmms = self
                .datastore
                .vmm_list_marked_stop_for_update(opctx, &p.current_pagparams())
                .await?;
            paginator = p.found_batch(&vmms, &|vmm| vmm.id);
            for vmm in vmms {
                let sled_id = vmm.sled_id();
                vmms_by_sled
                    .entry(sled_id)
                    .or_insert_with(|| SledVmmsToStop {
                        sled_id,
                        vmm_ids: Vec::new(),
                    })
                    .vmm_ids
                    .push(PropolisUuid::from_untyped_uuid(vmm.id));
            }
        }
        Ok(vmms_by_sled)
    }

    pub(crate) async fn actually_activate(
        &mut self,
        opctx: &OpContext,
    ) -> VmmStopForUpdateStatus {
        let vmms_by_sled = match self.vmms_to_stop_by_sled(opctx).await {
            Ok(vmms_by_sled) => vmms_by_sled,
            Err(err) => {
                slog::error!(
                    &opctx.log,
                    "failed to list VMMs marked to stop for a sled update";
                    &err,
                );
                return VmmStopForUpdateStatus {
                    vmms_stopped_by_sled: BTreeMap::new(),
                    error: Some(InlineErrorChain::new(&err).to_string()),
                };
            }
        };

        if vmms_by_sled.is_empty() {
            slog::debug!(
                &opctx.log,
                "no VMMs are marked to stop for a sled update";
            );
        } else {
            let vmm_count: usize =
                vmms_by_sled.iter().map(|sled| sled.vmm_ids.len()).sum();
            slog::info!(
                &opctx.log,
                "found VMMs marked to stop for a sled update";
                "sleds" => vmms_by_sled.len(),
                "vmms" => vmm_count,
            );
        }

        // TODO-K: actually stop the VMMs in `vmms_by_sled` and record
        // how many were stopped on each sled in `vmms_stopped_by_sled`.
        let vmms_stopped_by_sled = BTreeMap::new();

        VmmStopForUpdateStatus { vmms_stopped_by_sled, error: None }
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
