// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for pulling instance state from sled-agents.

use super::common::BackgroundTask;
use futures::{future::BoxFuture, FutureExt};
use nexus_db_queries::{
    context::OpContext, db::pagination::Paginator, db::DataStore,
};
use serde::Serialize;
use serde_json::json;
use std::num::NonZeroU32;
use std::sync::Arc;

/// Background task that periodically checks instance states.
pub struct InstanceWatcher {
    datastore: Arc<DataStore>,
}

const MAX_SLED_AGENTS: NonZeroU32 = unsafe {
    // Safety: last time I checked, 100 was greater than zero.
    NonZeroU32::new_unchecked(100)
};

impl BackgroundTask for InstanceWatcher {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            let latest_collection = {
                let maybe_id = self
                    .datastore
                    .inventory_get_latest_collection_id(opctx)
                    .await;
                match maybe_id {
                    Ok(Some(collection)) => collection,
                    Ok(None) => {
                        slog::debug!(opctx.log, "no inventory collection exists, not querying sled agents.");
                        return json!({});
                        }
                    Err(e) => {
                        slog::warn!(opctx.log, "failed to get latest collection ID: {e}");
                        return json!({});
                    }
                }
            };

            let mut requests = tokio::task::JoinSet::new();
            let mut paginator = Paginator::new(MAX_SLED_AGENTS);
            while let Some(p) = paginator.next() {
                let maybe_batch = self.datastore.sled_instance_list_by_sled_agent(
                    opctx,
                    latest_collection,
                    &p.current_pagparams(),
                ).await;
                let batch = match maybe_batch {
                    Ok(batch) => batch,
                    Err(e) => {
                        slog::warn!(opctx.log, "sled instances by sled agent query failed: {e}");
                        break;
                    }
                };
                paginator = p.found_batch(&batch, &|(sled_agent, _)| *sled_agent.sled_id);
                for (sled_agent, sled_instance) in batch {
                    todo!()
                }
            }

            todo!()
        }
        .boxed()
    }
}
